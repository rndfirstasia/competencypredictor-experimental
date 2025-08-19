import streamlit as st
import pandas as pd
import boto3
from datetime import datetime
import mysql.connector
from mysql.connector import Error #untuk menunjukkan error yang terjadi saat menghubungkan apliasi ke database

from openai import OpenAI
import openai
import requests
import io #data akan disimpan di ram sementara, digunakan agar datanya lebih cepat dan tidak perlu disimpan di file yang akan lebih lambat
from io import StringIO #stringio digunakan untuk data string sedangkan bytesio digunakan untuk data biner
from collections import defaultdict
import pytz #datetime sesuai zona waktu indon

import time
from requests.adapters import HTTPAdapter #dari lib requests, untuk merubah jumlah maksimal percakapan atau durasi waktu tunggu
from urllib3.util.retry import Retry #retry untuk timeout, maksutnya jika request gagal karena alasan tertentu maka retry akan mencoba untuk mengulang permintaan
import re #regex

import json

import os
import google.generativeai as genai

from pydub import AudioSegment
import concurrent.futures
import threading

#page config
st.set_page_config(
    page_icon="img/icon.png",
    page_title="Prediksi Kompetensi",
)

#env
#taruh semua credential ke secrets

#untuk deploy
genai.configure(api_key=st.secrets['gemini']['api'])
aws_access_key_id = st.secrets["aws"]["aws_access_key_id"]
aws_secret_access_key = st.secrets["aws"]["aws_secret_access_key"]
endpoint_url = st.secrets["aws"]["endpoint_url"]

mysql_user = st.secrets["mysql"]["username"]
mysql_password = st.secrets["mysql"]["password"]
mysql_host = st.secrets["mysql"]["host"]
mysql_port = st.secrets["mysql"]["port"]
mysql_database = st.secrets["mysql"]["database"]

openai.api_key=st.secrets["openai"]["api"]
client = OpenAI(api_key=st.secrets["openai"]["api"])
hf_token = st.secrets["hf"]["token"]
flask_url = st.secrets["flask"]["url"]
 #untuk API PITO
pito_url = st.secrets["sistem_fac"]["pito_url"]
vast_url = st.secrets["sistem_fac"]["vast_url"]
pito_api_user = st.secrets["sistem_fac"]["pito_api_user"]
pito_api_key = st.secrets["sistem_fac"]["pito_api_key"]
vast_api_user = st.secrets["sistem_fac"]["vast_api_user"]
vast_api_key = st.secrets["sistem_fac"]["vast_api_key"]

base_urls = {
    "PITO": pito_url,
    "VAST": vast_url
}

#function
def create_db_connection():
    try:
        conn = mysql.connector.connect(
            user=mysql_user,
            password=mysql_password,
            host=mysql_host,
            port=mysql_port,
            database=mysql_database
        )
        if conn.is_connected():
            return conn
        else:
            return None
    except Error as e:
        print(f"Error pada create_db_connection: {e}")
        return None

conn = create_db_connection()

if conn:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM txtan_assessor;')
    df_txtan_assessor = cursor.fetchall()
    column_name_txtan_assessor = [i[0] for i in cursor.description]
    df_txtan_assessor = pd.DataFrame(df_txtan_assessor, columns=column_name_txtan_assessor)

    cursor.execute("""
    SELECT
        pdc.id_product,                          
        pdc.name_product AS 'PRODUCT',
        comp.competency AS 'COMPETENCY',
        comp.description AS 'COMPETENCY DESCRIPTION',
        lvl.level_name AS 'LEVEL NAME',
        lvl.level_description AS 'LEVEL DESCRIPTION',
        comp.id_competency AS 'id_competency'
    FROM `pito_product` AS pdc
    JOIN pito_competency AS comp ON comp.id_product = pdc.id_product
    LEFT JOIN pito_competency_level AS lvl ON comp.id_competency = lvl.id_competency
    """)
    df_pito_product = cursor.fetchall()
    column_names_pito_product = [i[0] for i in cursor.description]
    df_pito_product = pd.DataFrame(df_pito_product, columns=column_names_pito_product)
    options_product_set = [""] + df_pito_product['PRODUCT'].drop_duplicates().tolist() #list produk dari database

    cursor.execute("""
    SELECT
        lvl.name_level AS 'NAMA LEVEL',
        lvl.value_level,
        lvl.id_level_set
    FROM pito_level AS lvl;
    """)
    df_pito_level = cursor.fetchall()
    column_names_pito_level = [i[0] for i in cursor.description]
    df_pito_level = pd.DataFrame(df_pito_level, columns=column_names_pito_level)
    options_level_set = [""] + df_pito_level['id_level_set'].drop_duplicates().tolist() #list level dari database
    cursor.close()
    conn.close()
else:
    st.error("Tidak bisa terhubung ke database")

st.header("Aplikasi Prediksi Kompetensi")

# Sidebar for navigation
st.sidebar.title("Parameter")
options_num_speaker = [ '2', '1', '3', '4', '5', '6']

#Sidebar
id_input_kode_assessor = st.sidebar.text_input("Kode Assessor Anda")
id_input_id_kandidat = st.sidebar.text_input("ID Kandidat")
selected_base_url = st.sidebar.selectbox("Pilih Sistem:", list(base_urls.keys()))
selected_option_num_speaker = st.sidebar.selectbox("Jumlah Speaker", options_num_speaker)
selected_option_product_set = st.sidebar.selectbox("Set Kompetensi", options_product_set)
selected_option_level_set = st.sidebar.selectbox("Set Level", options_level_set)
        
#connect API kandidat dengan PITO
if id_input_id_kandidat:
    headers = {
        "PITO": {
            "X-API-USER": pito_api_user,
            "X-API-KEY": pito_api_key
        },
        "VAST": {
            "X-API-USER": vast_api_user,
            "X-API-KEY": vast_api_key
        }
    }

    base_url = base_urls[selected_base_url]
    url = f"{base_url}{id_input_id_kandidat}"
    selected_headers = headers[selected_base_url]

    response_id_kandidat = requests.get(url, headers=selected_headers)

    if response_id_kandidat.status_code == 200:
        try:
            api_data = response_id_kandidat.json()
            st.session_state.response_id_kandidat = api_data
        except Exception as e:
            st.write(f"Error info id kandidat: {e}")
            
        api_id_kandidat = api_data["data"].get('id', 'Tidak tersedia')
        api_nama = api_data["data"].get('name', 'Tidak tersedia')
        api_jenis_kelamin = api_data["data"].get('gender', 'Tidak tersedia')
        api_produk = api_data["data"].get('product', 'Tidak tersedia')
        api_client = api_data["data"].get('client', 'Tidak tersedia')
        api_dob = api_data["data"].get('dob', 'Tidak tersedia')

        print(response_id_kandidat.text) #debug
        
        with st.container(border=True):
            st.write("#### Informasi ID Kandidat")
            
            st.write(f"ID Kandidat: {api_id_kandidat}")
            st.write(f"Nama: {api_nama}")
            st.write(f"Tanggal Lahir: {api_dob}")
            st.write(f"Jenis Kelamin: {api_jenis_kelamin}")
            st.write(f"Klien: {api_client}")
            st.write(f"Produk: {api_produk}")
        
    else:
        st.error(f"ID Kandidat tidak terdaftar/Sistem salah")
else:
    st.warning("Silakan masukkan ID Kandidat.")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Input Informasi", "📄 Hasil Transkrip", "🖨️ Hasil Prediksi", "⚙️ <admin> Input"])

########################TAB 1
########################TAB 1
with tab1:
    if not id_input_kode_assessor: #setting default kalau tidak ada kode assessor
        st.warning("Mohon masukkan kode Assessor Anda.")
    else:
        assessor_row = df_txtan_assessor[df_txtan_assessor['kode_assessor'].str.lower() == id_input_kode_assessor.lower()] #kode assessor bisa besar atau kecil

        if not assessor_row.empty:
            nama_assessor = assessor_row['name_assessor'].values[0]
            st.subheader(f"Selamat Datang, {nama_assessor}")
        else:
            st.subheader("Kode Assessor tidak terdaftar.")

    selected_product = df_pito_product[df_pito_product["PRODUCT"] == selected_option_product_set]
    with st.container(border=True):
        def get_levels_for_competency(id_competency):
            conn = create_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT level_name, level_description
                FROM pito_competency_level
                WHERE id_competency = %s
            """
            cursor.execute(query, (id_competency,))
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            levels = [{"level_name": row[0], "level_description": row[1]} for row in results]
            return levels
        
        competency_data = {}
        for _, row in df_pito_product.iterrows():
            if row['PRODUCT'] == selected_option_product_set:
                id_competency = row['id_competency']
                
                if id_competency not in competency_data:
                    competency_data[id_competency] = {
                        "product": row['PRODUCT'],
                        "competency": row['COMPETENCY'],
                        "description": row['COMPETENCY DESCRIPTION'],
                        "levels": []
                    }
                
                if row['LEVEL NAME'] and row['LEVEL DESCRIPTION']:
                    competency_data[id_competency]["levels"].append({
                        "level_name": row['LEVEL NAME'],
                        "level_description": row['LEVEL DESCRIPTION']
                    })

        competency_list = list(competency_data.values())
        
        if not selected_option_product_set:
            st.warning("Silahkan pilih set kompetensi")
        else:
            st.write(f'#### Set Kompetensi dari {selected_option_product_set}')
            if competency_list:
                for competency in competency_list:
                    st.write(f"##### {competency['competency']}")
                    if competency['description']:
                        st.write("Deskripsi:")
                        with st.container(border=True):
                            st.write(f"{competency['description']}")
                    else:
                        st.error('Error: Deskripsi kompetensi tidak ditemukan.', icon="🚨")
                    
                    if competency["levels"]:
                        st.write("Level:")
                        with st.container(border=True):
                            for level in competency["levels"]:
                                st.write(f"{level['level_name']}: {level['level_description']}")
                    else:
                        st.info('Info: Deskripsi level kompetensi tidak ditemukan.', icon="ℹ️")
            else:
                st.write(f"**Kompetensi tidak ditemukan.**")

    selected_level = df_pito_level[df_pito_level['id_level_set'] == selected_option_level_set]
    with st.container(border=True):
        #Level yang dipilih
        if not selected_option_level_set:
            st.warning("Silahkan pilih set level")
        else:
            st.write(f'#### Set Level dari {selected_option_level_set}')
            if not selected_level.empty:
                st.write(f"Terdiri dari:")
                with st.container(border=True):
                    for index, row in selected_level.iterrows():
                        st.write(f"**{row['value_level']}**. {row['NAMA LEVEL']}")
            else:
                st.error(f"Level set tidak ditemukan.", icon="🚨")

    #Tempat upload audio
    st.markdown("Upload File Audio Anda")
    audio_file = st.file_uploader("Pilih File Audio", type=["mp3", "m4a", "wav",])

    # Fungsi untuk mengambil transkrip
    def get_transcriptions(registration_id):
        conn = create_db_connection()
        if conn is None:
            st.error("Failed to connect to the database.")
            return []

        try:
            cursor = conn.cursor()
            query = """
            SELECT t.id_transkrip, t.registration_id, t.transkrip, t.speaker, t.start_section, t.end_section, a.num_speakers
            FROM txtan_transkrip t
            INNER JOIN txtan_audio a ON t.id_audio = a.id_audio
            WHERE t.registration_id = %s
            """
            cursor.execute(query, (registration_id,))  # Fix: remove the '1' parameter
            result = cursor.fetchall()
            return result

        except Exception as e:
            st.error(f"Error fetching transcriptions for registration_id {registration_id}: {e}")
            return []

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Fungsi untuk menyimpan ke tabel separator
    def insert_into_separator(id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section):
        conn = create_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO txtan_separator (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
        cursor.execute(query, values)

        print("Inserting into txtan_separator with values:", (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)) #debug

        conn.commit()
        cursor.close()
        conn.close()

    # Fungsi untuk menyimpan ke tabel result
    def insert_into_result(final_predictions_df, registration_id):
        conn = create_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO txtan_competency_result (registration_id, competency, level, reason)
        VALUES (%s, %s, %s, %s)
        """

        for index, row in final_predictions_df.iterrows():
            # ✅ Safe access with defaults
            competency = row.get('Kompetensi', f'Unknown_{index}')
            level = row.get('Level', 'Unknown')
            reason = row.get('Alasan', 'Alasan tidak tersedia')

            values = (registration_id, competency, level, reason)
            cursor.execute(query, values)

        conn.commit()
        cursor.close()
        conn.close()
        # st.success("Step 5/5: Prediksi dibuat, proses selesai.")

    # Fungsi untuk mengoreksi label pembicara
    def correct_speaker_labels(transkrip, num_speakers):
        prompt = (
            f"Berikut adalah transkrip dari percakapan interview dari {num_speakers} orang: \n"
            f"{transkrip}\n\n"
            "Dalam transkrip itu masih terdapat overlap antara Kandidat dan Assessor.\n"
            "Maka masukkan bagian yang overlap ke pembicara yang sebenarnya. Sehingga akan ada tanya jawab antar Assessor dan kandidat dan PASTI tidak hanya menjadi satu row.\n "
            "Jika orang lebih dari 2 maka akan ada lebih dari satu assessor. Kandidat tetap hanya akan ada satu.\n"
            "1. Kandidat (yang menjawab pertanyaan)\n"
            "2. Assessor (yang mengajukan pertanyaan)\n"
            "Contoh format dari bagian percakapan assessor dan kandidat:\n"
            "**Kandidat:** Untuk, misalkan contoh produknya ini sudah kita ekspor. Terus sudah kita coba untuk ekspor ke beberapa tempat, bagaimana supaya manajemen distribusinya (MD) itu produk ini dijalankan. Sudah kita ekspor, kita sesuaikan dengan promo yang mereka dari MD berikan. Karena kalau promonya tidak disesuaikan, secara otomatis produk ini nanti tidak akan terjual.\n"
            "**Assessor:** Kemudian, kalau dari sisi improvement, selama dua tahun terakhir ini boleh diceritakan seperti apa langkah improvement yang sudah pernah Bapak coba lakukan dan apakah inisiasinya dari diri Bapak sendiri? Ada contohnya seperti apa? Jika improvement terlalu banyak, seperti yang saya sampaikan tadi, karena kita lebih banyak, kalau saya sendiri.\n"
            "**Kandidat:** Kita lebih banyak ke ATM. Misalkan ada tim di tempat lain melakukan sesuatu, kita coba lakukan itu dengan sedikit modifikasi. Contohnya, kita selalu mengadakan yang namanya Red Light Promo. Itu salah satu usaha yang kita lakukan. Memang itu bukan gagasan dari saya, tapi gagasan dari beberapa toko. Tapi konsistensinya itu saya jalankan di tempat sini, konsistensi sebagaimana kita di tengah kondisi saat ini, contoh, trafik yang turun dan lain-lain, untuk menarik pelanggan yang datang ke toko, baik yang dari mal maupun yang dari luar. Itu yang saya konsistensikan dilakukan di toko ini.\n"
            "**Assessor:** Dengan melihat yang sudah dilakukan di toko-toko lain, jadi coba tetap konsisten dilakukan di tempat saat ini. Kalau misalkan dengan kondisi cabang saat ini, boleh diceritakan?.\n"
            "dan seterusnya.\n"
            "Tolong pastikan urutan dialog tetap seperti dalam transkrip asli, meskipun ada beberapa assessor.\n"
            "Betulkan juga bagian yang ada salah ketik atau ejaan yang kurang benar kecuali nama orang, nama perusahaan, nama jalan, nama kota, nama provinsi, nama negara, nama produk, singkatan.\n"
        )

        messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": prompt
                },
            ],
        }
        ]

        try:
            print("Sending request to API...") #debug
            response = openai.chat.completions.create(
                model="gpt-5-mini-2025-08-07",
                messages=messages
            )

            print("API Response:", response) #debug

            # Validasi respons dari API
            corrected_transcript = response.choices[0].message.content.strip()
            return corrected_transcript
            
        except Exception as e:
            st.error(f"Error while processing: {str(e)}")
            return None
    
    def process_gpt_response_to_dataframe(gpt_response):
        # FIXED: Check gpt_response instead of transcript
        if hasattr(gpt_response, 'text'):
            lines = gpt_response.text.split('\n')
        else:
            lines = gpt_response.split('\n')
        
        print(lines) #debug
        data = {'text': [], 'speaker': []}
        print(f"Data pada process gpt response: {data}") #debug

        for line in lines:
            if line.startswith("**Assessor:** ") or line.startswith("Assessor: ") or line.startswith("ASSESSOR: ") or line.startswith("**ASSESSOR**: ") or line.startswith("**ASSESSOR:** "):
                speaker = "Assessor"
                dialogue = line.replace("**Assessor:** ", "").replace("Assessor:", "").replace("ASSESSOR:", "").replace("**ASSESSOR**:", "").replace("**ASSESSOR:**", "").replace("****", "")
            elif line.startswith("**Kandidat:** ") or line.startswith("Kandidat: ") or line.startswith("KANDIDAT: ") or line.startswith("**KANDIDAT**: ") or line.startswith("**KANDIDAT:**"):
                speaker = "Kandidat"
                dialogue = line.replace("**Kandidat:** ", "").replace("Kandidat:", "").replace("KANDIDAT:", "").replace("**KANDIDAT**:", "").replace("**KANDIDAT:**", "").replace("****", "")
            else:
                continue
            data['text'].append(dialogue.strip())
            data['speaker'].append(speaker)
        
        df = pd.DataFrame(data)
        st.success("Pembicara berhasil ditambahkan.") #debug
        print(f"Process GPT response: {df}") #debug
        
        return df

    # Fungsi untuk memproses transkripsi
    def process_transcriptions(registration_id):
        transcriptions = get_transcriptions(registration_id)
        print(transcriptions) #debug

        if not transcriptions:
            st.error("No transcriptions found.")
            return
        
        transcriptions_by_registration = {}

        for transcription in transcriptions:
            reg_id = transcription[1]
            if reg_id not in transcriptions_by_registration:
                transcriptions_by_registration[reg_id] = []
            transcriptions_by_registration[reg_id].append(transcription)

        for registration_id, transcription_group in transcriptions_by_registration.items():
            combined_transcript = "\n".join([f"{t[3]}: {t[2]}" for t in transcription_group])
            num_speakers = transcription_group[0][6]

            print(f"Processing transcription for registration_id {registration_id}")  #debug
            print(combined_transcript) #debug

            corrected_transcript = correct_speaker_labels(combined_transcript, num_speakers)
            print(f"Corrected Transcript: {corrected_transcript}") #debug
            if not corrected_transcript:
                st.error(f"Corrected Transcript is None for registration_id {registration_id}")
                continue

            df = process_gpt_response_to_dataframe(corrected_transcript)
            print(df) #debug
            
            if df.empty:
                st.error(f"Empty DataFrame for registration_id {registration_id}.")
                continue
            
            print(f"Processed DataFrame for {registration_id}:", df)  #debug

            # Merger text dan speaker
            merged_text = []
            merged_speakers = []
            previous_speaker = None
            temp_text = ""
            temp_speaker = ""

            for _, row in df.iterrows():
                current_speaker = row['speaker']
                current_text = row['text']

                if current_speaker == previous_speaker:
                    temp_text += ' ' + current_text
                else:
                    if previous_speaker is not None:
                        merged_text.append(temp_text)
                        merged_speakers.append(temp_speaker)
                    
                    temp_text = current_text
                    temp_speaker = current_speaker
                    previous_speaker = current_speaker

            if temp_text:
                merged_text.append(temp_text)
                merged_speakers.append(temp_speaker)

            df_merged = pd.DataFrame({
                'text': merged_text,
                'speaker': merged_speakers
            })

            df_merged['text'] = df_merged['text'].replace(r'\s+', ' ', regex=True)

            for index, row in df_merged.iterrows():
                print(f"Inserting into txtan_separator: {row['text']}, {row['speaker']}") #debug
                insert_into_separator(
                    transcription_group[0][0], 
                    registration_id, 
                    row['text'], 
                    row['speaker'], 
                    transcription_group[0][4], 
                    transcription_group[0][5]
                )

            #st.success("Transcriptions processed and inserted.") #debug

    def update_transcription_status(id_audio):
        conn = create_db_connection()

        try:
                cursor = conn.cursor()

                update_query = '''
                    UPDATE txtan_audio
                    SET is_transcribed = 1
                    WHERE id_audio = %s
                '''
                cursor.execute(update_query, (id_audio,))
                conn.commit()
                print(f"Audio with id_audio {id_audio} marked as transcribed.")

        except Exception as e:
                print(f"Error: {e}")

    def get_separator(registration_id):
        conn = create_db_connection()
        cursor = conn.cursor()
        query = """
        SELECT s.id_transkrip, s.registration_id, s.revisi_transkrip, s.revisi_speaker, s.revisi_start_section, s.revisi_end_section
        FROM txtan_separator s
        INNER JOIN txtan_audio a ON s.registration_id = a.registration_id
        WHERE s.registration_id = %s
        """

        cursor.execute(query, (registration_id,))
        result = cursor.fetchall()

        print(f"Separator data fetched: {len(result)} entries for registration_id {registration_id}") #debug

        cursor.close()
        conn.close()
        return result            
    
    def get_competency(registration_id):
        conn = create_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT
                prd.name_product,
                comp.competency,
                comp.description,
                lvl.level_value,
                lvl.level_name,
                lvl.level_description
            FROM txtan_audio a
            JOIN pito_product prd ON prd.id_product = a.id_product
            JOIN pito_competency comp ON comp.id_product = prd.id_product
            LEFT JOIN pito_competency_level lvl ON lvl.id_competency = comp.id_competency
            WHERE a.registration_id = %s
        """
        
        cursor.execute(query, (registration_id,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Kembalikan hasil sebagai daftar dictionary agar mudah digunakan
        competencies = [{
            "product": row[0],
            "competency": row[1],
            "description": row[2],
            "level_value": row[3],
            "level_name": row[4],
            "level_description": row[5]
        } for row in result]
        
        return competencies

    def get_level_set_from_audio_table(registration_id):
            query = """
            SELECT a.id_level_set, lvl.name_level AS 'NAMA LEVEL'
            FROM txtan_audio a
            JOIN pito_level lvl ON a.id_level_set = lvl.id_level_set
            WHERE a.registration_id = %s
            """
            conn = create_db_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, (registration_id,))
                result = cursor.fetchone()
                cursor.fetchall()
                return result if result else (None, None)
            except Exception as e:
                print(f"Error fetching level set: {e}")
                return None, None
            finally:
                cursor.close()
                conn.close()

    id_level_set_fix, nama_level = get_level_set_from_audio_table(id_input_id_kandidat)
    df_pito_level['id_level_set'] = df_pito_level['id_level_set'].astype(str)
    df_pito_level['NAMA LEVEL'] = df_pito_level['NAMA LEVEL'].astype(str)

    if not id_level_set_fix:
        id_level_set_fix = selected_option_level_set
        nama_level = None

    id_level_set_fix = str(id_level_set_fix)
    filtered_levels_predict_competency = df_pito_level[df_pito_level['id_level_set'] == id_level_set_fix]
    level_names = filtered_levels_predict_competency['NAMA LEVEL'].tolist()
    print(f"Level names: {level_names}")  #debug
    print(f"Filtered levels predict competency: {filtered_levels_predict_competency}")  #debug
    dropdown_options_predict_competency = filtered_levels_predict_competency['NAMA LEVEL'].tolist()
    print(f"Dropdown options predict competency: {dropdown_options_predict_competency}") #debug
    print(dropdown_options_predict_competency)#debug

    #dapetin name_level dari table pito level untuk prediksi
    def get_name_levels_from_id_level_set(id_level_set):
        conn = create_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT name_level FROM pito_level
        WHERE id_level_set = %s
        """
        cursor.execute(query, (id_level_set,))
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        name_levels = [row[0] for row in result]

        return name_levels

    def predict_competency(combined_text, competencies, id_level_set):
        #name_levels = get_name_levels_from_id_level_set(id_level_set)

        prompt = "Saya memiliki transkrip hasil dari wawancara dan daftar kompetensi yang ingin diidentifikasi.\n\n"
        prompt += "Buatlah hasil analisa menjadi bentuk tabel dan prediksi juga levelnya.\n"
        prompt += "Hasil yang dikeluarkan WAJIB table dan TANPA FORMAT TEXT bold, italic atau sejenisnya.\n"

        prompt += "header kolom table HARUS menggunakan huruf kapital di awal dan dikuti dengan huruf kecil\n"

        prompt += f"Gunakan hanya level dari daftar berikut: {', '.join(level_names)}.\n" ### ini name levelnya belum ada
        prompt += "Pastikan level yang digunakan sesuai dengan level yang dipilih dan WAJIB DALAM BAHASA INGGRIS.\n"
        
        #prompt += "Level yang digunakan sesuai yang tercantum dibawah, semisal ada level 1 sampai level 5 maka level 5 adalah paling besar, atau jika ada very low sampai very high maka very high adalah paling besar. dan level WAJIB dalam bahasa inggris.\n"
        #prompt += f"Level yang digunakan juga mengikuti dari {dropdown_options_predict_competency} dan level WAJIB dalam bahasa inggris.\n"
        prompt += f"Teks transkrip berikut: {combined_text}\n\n"
        prompt += "Berikut adalah daftar kompetensi dengan level dan deskripsinya:\n"
        
        for competency in competencies:
            prompt += (f"- Kompetensi Bernama: {competency['competency']} deskripsinya adalah\n")
            
            #kalau ada level
            if competency.get("levels"):
                prompt += "  Level:\n"
                for level in competency["levels"]:
                    level_description = level["level_description"] if level["level_description"] else competency['description']
                    prompt += (f"    - Name: {level['level_name']}\n"
                            f"      Deskripsi Level: {level_description}\n")
            else:
                prompt += f"  (Tidak ada level spesifik, gunakan deskripsi kompetensi umum: {competency['description']})\n"
                # prompt += "Level yang digunakan adalah Very High, High, Medium, Low, Very Low dan level WAJIB dalam bahasa inggris.\n"
                prompt += f" Serta level mengikuti dari {level_names}."

        prompt += "\nHasil hanya akan berupa tabel dengan kolom: Kompetensi, Level, dan Alasan\n"
        
        print(f"Prompt: {prompt}") #debug

        messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": prompt
                },
            ],
        }
        ]

        try:
            start_time = time.time()
            response = openai.chat.completions.create(
                model="gpt-5-mini-2025-08-07",
                messages=messages
            )
            elapsed = time.time() - start_time
            print(f"OpenAI API call took {elapsed:.2f} seconds")
            print(f"Response: {response}")  # debug

            corrected_transcript_dict = response.model_dump()
            corrected_transcript = corrected_transcript_dict['choices'][0]['message']['content']
            return corrected_transcript

        except Exception as e:
            st.error(f"OpenAI API call failed: {e}")
            return None

    def combine_text_by_registration(separator_data):
        combined_data = defaultdict(lambda: {"revisi_transkrip": "", "revisi_speaker": ""})

        for record in separator_data:
            registration_id = record[1] #ini kuraang yakin harusnya dimulai dari 0 atau 1, nanti di cek
            revisi_transkrip = record[2] or ""
            revisi_speaker = record[3] or ""

            combined_data[registration_id]["revisi_transkrip"] += f" {revisi_transkrip}"
            combined_data[registration_id]["revisi_speaker"] += f" {revisi_speaker}"

        return combined_data

    def predictor(registration_id, dropdown_options_predict_competency):
        # Ambil data revisi dan kompetensi
        separator_data = get_separator(registration_id)
        print(f"Separator data: {separator_data}") #debug
        competency_data = get_competency(registration_id)
        print(f"Competency data: {competency_data}") #debug

        print(f"Fetched {len(separator_data)} separator data entries") #debug
        print(f"Fetched {len(competency_data)} competency data entries") #debug

        if not separator_data:
            st.error("No data found in the separator table.")
            return

        if not competency_data:
            st.error("No competency data found.")
            return

        competency_list = [{"competency": row.get("competency"), 
                            "description": row.get("description"),
                            **({
                                "level_value": row.get("level_value"),
                                "level_name": row.get("level_name"),
                                "level_description": row.get("level_description")
                                }if row.get("level_value") and row.get("level_name") and row.get("level_description") else {})
                            } 
                            for row in competency_data]
        print(f"Competency list: {competency_list}") #debug

        combined_data = combine_text_by_registration(separator_data)
        print(f"combined_data: {combined_data}") #debug

        all_predictions = []

        for registration_id, text_data in combined_data.items():
            combined_text = f"{text_data['revisi_transkrip']} {text_data['revisi_speaker']}"

            # st.success(f"Step 4/5: Mohon tunggu, proses prediksi berlangsung.....") #debug

            predicted_competency = predict_competency(combined_text, competency_list, level_names)

            print(f"Predicted competency for {registration_id}:\n{predicted_competency}") #debug

            try:
                df_competency = pd.read_csv(StringIO(predicted_competency), sep='|', skipinitialspace=True)
                df_competency.columns = df_competency.columns.str.strip()
                df_competency['registration_id'] = registration_id
                # st.success(f"Step 4/5: Mohon tunggu, proses prediksi berlangsung.....") #debug

                all_predictions.append(df_competency)

            except Exception as e:
                st.error(f"Error processing prediction for registration ID {registration_id}: {e}")
        
        print(all_predictions) #debug

        if all_predictions:
            print(f"all_predictions before: {all_predictions}")  # debug
            
            if isinstance(all_predictions, list) and all(isinstance(df, pd.DataFrame) for df in all_predictions):
                final_predictions_df = pd.concat(all_predictions, ignore_index=True)
                #st.dataframe(f"Final pred CONCAT: {final_predictions_df}") #debug
                final_predictions_df = final_predictions_df.applymap(lambda x: x.replace('**', '') if isinstance(x, str) else x)
                #st.dataframe(f"Final pred MAP: {final_predictions_df}") #debug
                final_predictions_df = final_predictions_df.drop(index=0).reset_index(drop=True)
                #st.dataframe(f"Final pred DROP dan RESET INDEX: {final_predictions_df}") #debug
                
                print(f"Final pred DONE: {final_predictions_df}")  # debug
                
                insert_into_result(final_predictions_df, registration_id)
            else:
                st.error("Error: all_predictions harus berupa list yang berisi DataFrame.")
        else:
            st.error("Error: all_predictions kosong.")

    #ambil data hasil transkrip pada 
    def fetch_transkrip_from_db(registration_id):
        conn = create_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT transkrip, speaker, start_section, end_section
        FROM txtan_transkrip
        WHERE registration_id = %s
        """
        cursor.execute(query, (registration_id,))
        transkrip_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return transkrip_data

    class NamedBytesIO(io.BytesIO):
        def __init__(self, content, name):
            super().__init__(content)
            self.name = name 

    def transcribe_with_whisper(audio_file):
        if not audio_file:
            raise ValueError("File audio tidak diberikan")
        
        if hasattr(audio_file, 'name'):
            audio_file_name = audio_file.name
        else:
            raise ValueError("Objek audio tidak memiliki atribut nama file")

        st.write(f"Mengirim file ke Whisper API: {audio_file_name}")

        audio_bytes = audio_file.getvalue()
        
        audio_file_whisper = NamedBytesIO(audio_bytes, audio_file_name)

        transcript = client.audio.transcriptions.create(
            model="whisper-1",
            file=(audio_file_name, audio_file_whisper, "audio/m4a"),  
            response_format="text"
        )

        return transcript

    def separate_speakers(transcript, num_speakers=2):
        prompt = f"""
        Berikut adalah transkrip wawancara dengan {num_speakers} orang.
        Pisahkan dialog berdasarkan peran:
        - **Kandidat** (yang menjawab pertanyaan)
        - **Assessor** (yang bertanya)
        
        Transkripsi: {transcript}
        
        Format keluaran:
        **Kandidat:** [isi dialog]
        **Assessor:** [isi dialog]
        """

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    
    def transcribe_with_whisper(audio_file):
        if not audio_file:
            raise ValueError("File audio tidak diberikan")
        
        if hasattr(audio_file, 'name'):
            audio_file_name = audio_file.name
        else:
            raise ValueError("Objek audio tidak memiliki atribut nama file")
        
        st.write(f"Mengirim file ke Whisper API: {audio_file_name}")
        
        audio_bytes = audio_file.getvalue()
        
        class NamedBytesIO(io.BytesIO):
            def __init__(self, content, name):
                super().__init__(content)
                self.name = name
        
        audio_file_whisper = NamedBytesIO(audio_bytes, audio_file_name)
        
        transcript = client.audio.transcriptions.create(
            model="gpt-4o-transcribe",
            file=(audio_file_name, audio_file_whisper, "audio/m4a"),
            response_format="text"
        )
        
        process_gpt_response_to_dataframe(transcript)
        st.success("Step 2/5: Audio berhasil dikirim untuk transkripsi.")
        return transcript
    
    def transcribe_audio_gemini(audio_file_path, model_name="gemini-1.5-pro-latest"):
        try:
            try:
                uploaded_file = genai.upload_file(audio_file_path)
            except Exception as upload_error:
                st.error(f"Error during file upload to Gemini API: {upload_error}")
                return None

            model = genai.GenerativeModel(model_name)

            response = model.generate_content(['Transkrip audio ini\n Hasilnya WAJIB dipisahkan dengan format seperti\n **Assessor:** TEXT TRANSKRIP \n **Kandidat:** TEXT TRANSKRIP\n **Assessor:** TEXT TRANSKRIP\n **Kandidat:** TEXT TRANSKRIP\n dan seterusnya', uploaded_file])

            if response.prompt_feedback and response.prompt_feedback.block_reason:
                st.error(f"Transkripsi diblokir karena: {response.prompt_feedback.block_reason}")
                return None

            if response.text:
                st.success("Step 2/5: Audio berhasil dikirim untuk transkripsi.")
                return response.text
            else:
                st.warning("Tidak ada teks yang dihasilkan dari Gemini API.")
                return None

        except FileNotFoundError:
            st.error(f"Error: File audio tidak ditemukan: {audio_file_path}")
            return None
        except Exception as e:
            st.error(f"Error selama transkripsi dengan Gemini API: {e}")
            return None
        
    def insert_into_separator(id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section):
        conn = create_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO txtan_separator (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
        cursor.execute(query, values)

        conn.commit()
        cursor.close()
        conn.close()

# ============================ MAINTENANCE TRANSCRIBE ============================================
    def estimate_audio_duration(audio_bytes):
        """Estimate audio duration from bytes"""
        try:
            audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
            duration_minutes = len(audio) / 1000 / 60
            return duration_minutes
        except:
            # Fallback estimation based on file size
            file_size_mb = len(audio_bytes) / (1024 * 1024)
            estimated_minutes = file_size_mb * 0.8  # Rough estimation
            return estimated_minutes

    def should_use_chunked_transcription(audio_bytes, file_size_threshold_mb=25, duration_threshold_minutes=25):
        """Determine if chunked transcription should be used"""
        file_size_mb = len(audio_bytes) / (1024 * 1024)
        estimated_duration = estimate_audio_duration(audio_bytes)
        
        # st.info(f"📊 File size: {file_size_mb:.1f} MB, Estimated duration: {estimated_duration:.1f} minutes")
        
        use_chunking = file_size_mb > file_size_threshold_mb or estimated_duration > duration_threshold_minutes
        
        # if use_chunking:
        #     st.info("📦 Large file detected - Using chunked transcription for better accuracy")
        # else:
        #     st.info("📝 Regular file size - Using standard transcription")
        
        return use_chunking

    def split_audio_for_transcription(audio_bytes, chunk_duration_minutes=10, overlap_seconds=30):
        """Split audio into chunks for transcription"""
        try:
            audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
            
            chunk_duration_ms = chunk_duration_minutes * 60 * 1000
            overlap_ms = overlap_seconds * 1000
            
            chunks = []
            start = 0
            
            st.info(f"🔪 Splitting {len(audio)/1000/60:.1f} min audio into {chunk_duration_minutes}min chunks")
            
            while start < len(audio):
                end = min(start + chunk_duration_ms, len(audio))
                chunk = audio[start:end]
                
                # Export chunk to bytes
                chunk_buffer = io.BytesIO()
                chunk.export(chunk_buffer, format="mp3")
                
                chunks.append({
                    'bytes': chunk_buffer.getvalue(),
                    'start_time': start / 1000,
                    'end_time': end / 1000,
                    'index': len(chunks)
                })
                
                if end >= len(audio):
                    break
                start = end - overlap_ms
            
            st.success(f"✅ Created {len(chunks)} audio chunks")
            return chunks
            
        except Exception as e:
            st.error(f"❌ Error splitting audio: {e}")
            return None

    def transcribe_with_openai_chunked(audio_bytes, file_name):
        try:
            # Convert to raw bytes
            if hasattr(audio_bytes, 'read'):
                audio_bytes.seek(0)
                raw_bytes = audio_bytes.read()
            elif hasattr(audio_bytes, 'getvalue'):
                raw_bytes = audio_bytes.getvalue()
            else:
                raw_bytes = audio_bytes
            
            print(f"DEBUG: Audio size: {len(raw_bytes)} bytes")
            use_chunking = should_use_chunked_transcription(raw_bytes)

            if not use_chunking:
                # st.info("🎙️ Transcribing directly...")
                audio_file = io.BytesIO(raw_bytes)
                audio_file.name = file_name
                response = openai.audio.transcriptions.create(
                    model="gpt-4o-transcribe",
                    file=audio_file
                )
                return response.text  # ✅ Kembalikan string saja

            # Chunked
            chunks = split_audio_for_transcription(raw_bytes)
            if not chunks:
                raise Exception("Failed to split audio")

            print(f"DEBUG: {len(chunks)} chunks created")
            chunk_transcripts = []
            progress_bar = st.progress(0)

            for i, chunk in enumerate(chunks):
                try:
                    st.info(f"🔄 Transcribing chunk {i+1}/{len(chunks)}")
                    audio_file = io.BytesIO(chunk['bytes'])
                    audio_file.name = f"chunk_{i}.mp3"

                    response = openai.audio.transcriptions.create(
                        model="gpt-4o-transcribe",
                        file=audio_file
                    )

                    chunk_transcripts.append(response.text)  # ✅ Append string langsung
                    progress_bar.progress((i + 1) / len(chunks))
                    st.success(f"✅ Chunk {i+1} done ({len(response.text)} chars)")
                except Exception as e:
                    st.error(f"❌ Chunk {i+1} failed: {e}")
                    chunk_transcripts.append("")

            if any(chunk_transcripts):
                return "\n".join(chunk_transcripts)  # ✅ Combine string list
            else:
                raise Exception("All chunks failed")
        except Exception as e:
            st.error(f"❌ Transcription failed: {e}")
            return None

    def combine_chunked_transcripts(chunk_transcripts):
        """Combine chunk transcripts using GPT-4o-mini"""
        try:
            if not chunk_transcripts:
                return ""
            
            # Extract text from Transcription objects and filter out empty ones
            valid_transcripts = []
            for t in chunk_transcripts:
                # Handle both Transcription objects and strings
                if hasattr(t, 'text'):
                    # OpenAI Transcription object
                    text = t.text.strip()
                elif hasattr(t, 'strip'):
                    # Already a string
                    text = t.strip()
                else:
                    # Convert to string first
                    text = str(t).strip()
                
                if text:  # Only add non-empty transcripts
                    valid_transcripts.append(text)
            
            # If no valid transcripts, return empty
            if not valid_transcripts:
                return ""
            
            # If only one transcript, return it directly
            if len(valid_transcripts) == 1:
                return valid_transcripts[0]
            
            st.info("🔗 Combining transcript chunks with GPT-4o-mini...")
            
            # Combine transcripts intelligently
            combined = valid_transcripts[0]
            
            for i in range(1, len(valid_transcripts)):
                current_chunk = valid_transcripts[i]
                
                prompt = f"""Gabungkan dua bagian transkrip wawancara ini dengan menghilangkan duplikasi.

                BAGIAN SEBELUMNYA (akhir):
                {combined[-800:]}

                BAGIAN SELANJUTNYA:
                {current_chunk[:800]}

                ATURAN:
                1. Hilangkan kalimat yang duplikat/overlap
                2. Pastikan transisi alami antar bagian
                3. Pertahankan konteks percakapan
                4. Kembalikan hasil gabungan yang bersih

                HASIL GABUNGAN:"""

                try:
                    response = openai.chat.completions.create(
                        model="gpt-5-mini-2025-08-07",
                        messages=[{"role": "user", "content": prompt}],
                        timeout=30
                    )
                    
                    resolved_text = response.choices[0].message.content.strip()
                    combined = combined[:-800] + resolved_text
                    
                except Exception as e:
                    st.warning(f"⚠️ Could not resolve overlap for chunk {i+1}: {e}")
                    # Fallback: simple concatenation
                    combined += "\n" + current_chunk
            
            st.success("✅ Transcript chunks combined successfully")
            return combined
            
        except Exception as e:
            st.error(f"❌ Error combining transcripts: {e}")
            
            # Emergency fallback: extract text and join with newlines
            try:
                fallback_texts = []
                for t in chunk_transcripts:
                    if hasattr(t, 'text'):
                        text = t.text.strip()
                    elif hasattr(t, 'strip'):
                        text = t.strip()
                    else:
                        text = str(t).strip()
                    
                    if text:
                        fallback_texts.append(text)
                
                return "\n".join(fallback_texts)
                
            except Exception as fallback_error:
                st.error(f"❌ Fallback failed too: {fallback_error}")
                return ""

    def separate_speakers_gpt4o_mini(transcript, num_speakers=2):
        """Separate speakers using GPT-4o-mini"""
        try:
            st.info("🎭 Separating speakers with GPT-4o-mini...")
            
            # PERBAIKAN: Ekstrak text dari objek Transcription
            if hasattr(transcript, 'text'):
                transcript_text = transcript.text
            else:
                transcript_text = str(transcript)  # Fallback jika bukan objek Transcription
            
            speaker_labels = ["Assessor", "Kandidat"] if num_speakers == 2 else [f"Speaker {i+1}" for i in range(num_speakers)]
            
            # Split long transcripts into chunks for processing
            max_chars = 12000
            if len(transcript_text) > max_chars:  # Gunakan transcript_text, bukan transcript
                chunks = [transcript_text[i:i+max_chars] for i in range(0, len(transcript_text), max_chars-500)]
                separated_chunks = []
                
                for i, chunk in enumerate(chunks):
                    st.info(f"🎭 Processing speaker separation chunk {i+1}/{len(chunks)}")
                    separated_chunk = process_speaker_separation_chunk(chunk, speaker_labels, i > 0)
                    separated_chunks.append(separated_chunk)
                
                final_result = "\n".join(separated_chunks)
            else:
                final_result = process_speaker_separation_chunk(transcript_text, speaker_labels, False)  # Gunakan transcript_text
            
            #st.success("✅ Speaker separation completed")
            return final_result
            
        except Exception as e:
            st.error(f"❌ Speaker separation failed: {e}")
            # Return text version untuk fallback
            if hasattr(transcript, 'text'):
                return transcript.text
            else:
                return str(transcript)

    def process_speaker_separation_chunk(text_chunk, speaker_labels, is_continuation=False):
        """Process a single chunk for speaker separation"""
        
        continuation_note = "\nCATATAN: Ini adalah lanjutan dari bagian sebelumnya." if is_continuation else ""
        
        prompt = f"""Analisis transkrip wawancara dan pisahkan berdasarkan pembicara.

        PEMBICARA: {', '.join(speaker_labels)}

        ATURAN WAJIB:
        1. Format: **NamaPembicara:** teks
        2. Assessor: bertanya, mengevaluasi, memberikan instruksi
        3. Kandidat: menjawab, menjelaskan pengalaman
        4. Pisahkan berdasarkan konteks dan pola bicara
        5. Jangan tambahkan komentar atau penjelasan{continuation_note}

        CONTOH FORMAT:
        **Assessor:** Selamat pagi, silakan perkenalkan diri Anda.
        **Kandidat:** Selamat pagi, nama saya Ahmad dan saya lulusan IT.

        TRANSKRIP:
        {text_chunk}

        HASIL PEMISAHAN:"""

        try:
            response = openai.chat.completions.create(
                model="gpt-5-mini-2025-08-07",
                messages=[{"role": "user", "content": prompt}],
                timeout=120
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            st.error(f"❌ Error in speaker separation chunk: {e}")
            return text_chunk
        
    def transcribe_single_chunk(chunk_data):
        """
        Transcribe satu chunk - untuk dijalankan paralel
        """
        chunk_index, chunk_bytes = chunk_data
        
        try:
            audio_file = io.BytesIO(chunk_bytes)
            audio_file.name = f"chunk_{chunk_index}.mp3"

            response = openai.audio.transcriptions.create(
                model="gpt-4o-transcribe",
                file=audio_file
            )
            
            return {
                "index": chunk_index,
                "success": True,
                "text": response.text,
                "chars": len(response.text)
            }
        except Exception as e:
            return {
                "index": chunk_index,
                "success": False,
                "text": "",
                "error": str(e)
            }

    def transcribe_with_openai_chunked_parallel(audio_bytes, file_name, max_workers=4):
        """
        Versi paralel dari transcribe_with_openai_chunked yang sudah ada
        """
        try:
            # Convert to raw bytes (sama seperti sebelumnya)
            if hasattr(audio_bytes, 'read'):
                audio_bytes.seek(0)
                raw_bytes = audio_bytes.read()
            elif hasattr(audio_bytes, 'getvalue'):
                raw_bytes = audio_bytes.getvalue()
            else:
                raw_bytes = audio_bytes
            
            print(f"DEBUG: Audio size: {len(raw_bytes)} bytes")
            use_chunking = should_use_chunked_transcription(raw_bytes)

            if not use_chunking:
                #st.info("🎙️ Transcribing directly...")
                audio_file = io.BytesIO(raw_bytes)
                audio_file.name = file_name
                response = openai.audio.transcriptions.create(
                    model="gpt-4o-transcribe",
                    file=audio_file
                )
                return response.text

            # Chunked (sama seperti sebelumnya)
            chunks = split_audio_for_transcription(raw_bytes)
            if not chunks:
                raise Exception("Failed to split audio")

            print(f"DEBUG: {len(chunks)} chunks created")
            
            # === BAGIAN YANG DIUBAH: PARALEL PROCESSING ===
            #st.info(f"🚀 Starting parallel transcription of {len(chunks)} chunks...")
            progress_bar = st.progress(0)
            status_placeholder = st.empty()
            
            # Prepare chunk data for parallel processing
            chunk_data_list = [(i, chunk['bytes']) for i, chunk in enumerate(chunks)]
            
            # Process chunks in parallel
            chunk_results = [None] * len(chunks)  # Maintain order
            completed = 0
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all chunks
                future_to_index = {
                    executor.submit(transcribe_single_chunk, chunk_data): chunk_data[0] 
                    for chunk_data in chunk_data_list
                }
                
                # Process completed futures
                for future in concurrent.futures.as_completed(future_to_index):
                    chunk_index = future_to_index[future]
                    
                    try:
                        result = future.result()
                        chunk_results[result["index"]] = result
                        
                        completed += 1
                        progress_bar.progress(completed / len(chunks))
                        
                        if result["success"]:
                            status_placeholder.success(f"✅ Chunk {result['index']+1}/{len(chunks)} done ({result['chars']} chars)")
                        else:
                            status_placeholder.error(f"❌ Chunk {result['index']+1}/{len(chunks)} failed: {result['error']}")
                            
                    except Exception as exc:
                        completed += 1
                        progress_bar.progress(completed / len(chunks))
                        status_placeholder.error(f"❌ Chunk {chunk_index+1}/{len(chunks)} exception: {exc}")
                        
                        # Create failed result
                        chunk_results[chunk_index] = {
                            "index": chunk_index,
                            "success": False,
                            "text": "",
                            "error": str(exc)
                        }
            
            # Combine results in order (sama seperti sebelumnya)
            chunk_transcripts = []
            for result in chunk_results:
                if result and result["success"]:
                    chunk_transcripts.append(result["text"])
                else:
                    chunk_transcripts.append("")  # Empty untuk chunk yang gagal
            
            if any(chunk_transcripts):
                final_transcript = "\n".join(chunk_transcripts)
                
                # Show summary
                successful_chunks = sum(1 for r in chunk_results if r and r["success"])
                st.success(f"🎉 Parallel transcription completed: {successful_chunks}/{len(chunks)} chunks successful")
                
                return final_transcript
            else:
                raise Exception("All chunks failed")
                
        except Exception as e:
            st.error(f"❌ Parallel transcription failed: {e}")
            return None

    # MODIFIKASI MINIMAL PADA KODE UTAMA
    # Ganti saja pemanggilan fungsi di bagian ini:

    def transcribe_with_gpt_first_parallel():
        """Transcription function - GPT/OpenAI first priority with parallel processing"""
        try:
            # Force GPT/OpenAI Whisper first - SEKARANG PARALEL
            transcript = transcribe_with_openai_chunked_parallel(audio_file_bytes, file_name, max_workers=4)
            
            if transcript and len(transcript.strip()) > 10:
                return {
                    "success": True, 
                    "transcript": transcript,
                    "method": "OpenAI Transcriber (GPT) - Parallel"
                }
            else:
                # Fallback to Gemini if GPT fails (tetap sama)
                temp_filename = f"temp_audio_fallback.{file_extension}"
                with open(temp_filename, "wb") as temp_file:
                    temp_file.write(audio_file_bytes)
                
                transcript = transcribe_audio_gemini(temp_filename)
                
                # Clean up temp file
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
                
                if transcript and len(transcript.strip()) > 10:
                    return {
                        "success": True,
                        "transcript": transcript, 
                        "method": "Gemini API (Fallback)"
                    }
                else:
                    return {"success": False, "error": "All transcription methods failed"}
                    
        except Exception as e:
            return {"success": False, "error": str(e)}
        
    def split_transcript_for_separation(transcript_text, chunk_size=2000):
        """
        Split transkrip menjadi chunks untuk parallel speaker separation
        
        Args:
            transcript_text: Full transcript text
            chunk_size: Approximate WORDS per chunk (not characters)
        
        Returns:
            List of tuples: [(chunk_index, chunk_text), ...]
        """
        # Split by sentences to maintain context
        sentences = transcript_text.split('. ')
        
        chunks = []
        current_chunk = ""
        current_word_count = 0
        chunk_index = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            # Add sentence to current chunk
            if current_chunk:
                test_chunk = current_chunk + ". " + sentence
                test_word_count = current_word_count + sentence_words + 1  # +1 for the period and space
            else:
                test_chunk = sentence
                test_word_count = sentence_words
            
            # If chunk gets too big (in WORDS), save current and start new one
            if test_word_count > chunk_size and current_chunk:
                chunks.append((chunk_index, current_chunk))
                current_chunk = sentence
                current_word_count = sentence_words
                chunk_index += 1
            else:
                current_chunk = test_chunk
                current_word_count = test_word_count
        
        # Add last chunk
        if current_chunk:
            chunks.append((chunk_index, current_chunk))
        
        return chunks

    def separate_speakers_single_chunk(chunk_data, num_speakers):
        """
        Separate speakers for a single transcript chunk
        
        Args:
            chunk_data: Tuple of (chunk_index, chunk_text)
            num_speakers: Number of speakers to separate
        
        Returns:
            Dictionary with separation result
        """
        chunk_index, chunk_text = chunk_data
        
        try:
            # Call your existing speaker separation function
            separated_text = separate_speakers_gpt4o_mini(chunk_text, num_speakers)
            
            if separated_text and len(separated_text.strip()) > 10:
                return {
                    "success": True,
                    "chunk_index": chunk_index,
                    "separated_text": separated_text,
                    "original_length": len(chunk_text),
                    "separated_length": len(separated_text)
                }
            else:
                return {
                    "success": False,
                    "chunk_index": chunk_index,
                    "error": "Empty or invalid separation result",
                    "fallback_text": chunk_text  # Use original if separation fails
                }
                
        except Exception as e:
            return {
                "success": False,
                "chunk_index": chunk_index,
                "error": str(e),
                "fallback_text": chunk_text
            }

    def separate_speakers_parallel(transcript_text, num_speakers, max_workers=3):
        """
        Parallel speaker separation for interview transcripts
        
        Args:
            transcript_text: Full transcript text
            num_speakers: Number of speakers
            max_workers: Maximum parallel workers (lower for GPT API limits)
        
        Returns:
            Combined separated transcript
        """
        
        print("🔍 DEBUG: Entering separate_speakers_parallel()")
        print(f"🔍 DEBUG: transcript_text type: {type(transcript_text)}")
        print(f"🔍 DEBUG: transcript_text length: {len(transcript_text) if transcript_text else 'None/Empty'}")
        print(f"🔍 DEBUG: num_speakers: {num_speakers}")
        print(f"🔍 DEBUG: max_workers: {max_workers}")
        
        if num_speakers <= 1:
            #st.info("👤 Single speaker - skipping separation")
            print("🔍 DEBUG: Returning original transcript (single speaker)")
            return transcript_text
        
        # st.info(f"🎭 Starting parallel speaker separation for {num_speakers} speakers...")
        
        try:
            print("🔍 DEBUG: About to call split_transcript_for_separation()")
            # Split transcript into chunks
            chunks = split_transcript_for_separation(transcript_text, chunk_size=1500)
            print(f"🔍 DEBUG: split_transcript_for_separation() returned {len(chunks)} chunks")
            # st.info(f"📋 Split transcript into {len(chunks)} chunks for parallel separation")
            
        except Exception as chunk_error:
            st.error(f"❌ Error in split_transcript_for_separation(): {chunk_error}")
            import traceback
            st.code(traceback.format_exc())
            return transcript_text  # Fallback to original
        
        if len(chunks) <= 1:
            # If only 1 chunk, use original function
            # st.info("📝 Single chunk - using direct separation")
            print("🔍 DEBUG: About to call separate_speakers_gpt4o_mini()")
            try:
                result = separate_speakers_gpt4o_mini(transcript_text, num_speakers)
                print(f"🔍 DEBUG: separate_speakers_gpt4o_mini() returned: {type(result)}")
                return result
            except Exception as gpt_error:
                st.error(f"❌ Error in separate_speakers_gpt4o_mini(): {gpt_error}")
                import traceback
                st.code(traceback.format_exc())
                return transcript_text  # Fallback to original
        
        # Progress tracking
        print("🔍 DEBUG: Setting up progress tracking")
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Parallel speaker separation
        separation_results = [None] * len(chunks)
        completed = 0
        
        print("🔍 DEBUG: Starting ThreadPoolExecutor")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                print("🔍 DEBUG: Submitting chunks for separation")
                # Submit all chunks for separation
                future_to_index = {}
                for i, chunk in enumerate(chunks):
                    try:
                        future = executor.submit(separate_speakers_single_chunk, chunk, num_speakers)
                        future_to_index[future] = i  # Use index instead of chunk[0]
                        print(f"🔍 DEBUG: Submitted chunk {i} for separation")
                    except Exception as submit_error:
                        st.error(f"❌ Error submitting chunk {i}: {submit_error}")
                
                print(f"🔍 DEBUG: {len(future_to_index)} chunks submitted successfully")
                
                # Process completed futures
                for future in concurrent.futures.as_completed(future_to_index):
                    chunk_index = future_to_index[future]
                    print(f"🔍 DEBUG: Processing completed future for chunk {chunk_index}")
                    
                    try:
                        result = future.result()
                        print(f"🔍 DEBUG: Future result type: {type(result)}")
                        print(f"🔍 DEBUG: Future result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
                        
                        separation_results[result["chunk_index"]] = result
                        
                        completed += 1
                        progress_bar.progress(completed / len(chunks))
                        
                        if result["success"]:
                            status_placeholder.success(f"✅ Speaker separation {result['chunk_index']+1}/{len(chunks)} completed")
                        else:
                            status_placeholder.warning(f"⚠️ Speaker separation {result['chunk_index']+1}/{len(chunks)} failed: {result['error']}")
                            
                    except Exception as exc:
                        st.error(f"❌ Exception processing chunk {chunk_index}: {exc}")
                        import traceback
                        st.code(traceback.format_exc())
                        
                        completed += 1
                        progress_bar.progress(completed / len(chunks))
                        status_placeholder.error(f"❌ Speaker separation {chunk_index+1}/{len(chunks)} exception: {exc}")
                        
                        # Create fallback result
                        separation_results[chunk_index] = {
                            "success": False,
                            "chunk_index": chunk_index,
                            "error": str(exc),
                            "fallback_text": chunks[chunk_index][1] if len(chunks[chunk_index]) > 1 else str(chunks[chunk_index])
                        }
                
        except Exception as executor_error:
            st.error(f"❌ Error in ThreadPoolExecutor: {executor_error}")
            import traceback
            st.code(traceback.format_exc())
            return transcript_text  # Fallback to original
        
        # Combine results in order
        # st.info("🔗 Combining separated chunks in correct order...")
        print(f"🔍 DEBUG: separation_results length: {len(separation_results)}")
        print(f"🔍 DEBUG: None results: {sum(1 for r in separation_results if r is None)}")
        
        combined_separated = ""
        successful_chunks = 0
        
        for i, result in enumerate(separation_results):
            print(f"🔍 DEBUG: Processing result {i}: {result is not None}")
            if result:
                if result["success"]:
                    text_to_add = result["separated_text"]
                    successful_chunks += 1
                    print(f"🔍 DEBUG: Added successful chunk {i}")
                else:
                    # Use original text if separation failed
                    text_to_add = result["fallback_text"]
                    print(f"🔍 DEBUG: Added fallback chunk {i}")
                
                # Add proper spacing between chunks
                if combined_separated and not combined_separated.endswith("\n"):
                    combined_separated += "\n\n"
                
                combined_separated += text_to_add
        
        # Clean up combined result
        combined_separated = combined_separated.strip()
        
        print(f"🔍 DEBUG: Final combined_separated length: {len(combined_separated)}")
        
        # Show summary
        success_rate = successful_chunks / len(chunks) * 100
        # st.success(f"""
        # 🎭 **Parallel Speaker Separation Completed!**
        
        # 📊 **Results:**
        # - Successful chunks: {successful_chunks}/{len(chunks)}
        # - Success rate: {success_rate:.1f}%
        # - Total speakers: {num_speakers}
        # - Final transcript length: {len(combined_separated)} characters
        # """)
        
        print("🔍 DEBUG: Returning combined_separated")
        return combined_separated
    
    def split_audio_into_chunks(audio_bytes, chunk_duration_ms=600000, overlap_ms=1000):
        """
        Split audio into chunks for parallel processing
        
        Args:
            audio_bytes: Raw audio file bytes
            chunk_duration_ms: Duration of each chunk in milliseconds (default 30 seconds)
            overlap_ms: Overlap between chunks in milliseconds (default 1 second)
        
        Returns:
            List of tuples: [(chunk_index, chunk_bytes), ...]
        """
        try:
            from pydub import AudioSegment
            
            # Load audio from bytes
            audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
            
            chunks = []
            start = 0
            chunk_index = 0
            
            while start < len(audio):
                # Calculate end position
                end = min(start + chunk_duration_ms, len(audio))
                
                # Extract chunk with overlap for continuity
                if chunk_index > 0:
                    chunk_start = max(0, start - overlap_ms)
                else:
                    chunk_start = start
                    
                if end < len(audio):
                    chunk_end = min(len(audio), end + overlap_ms)
                else:
                    chunk_end = end
                
                chunk = audio[chunk_start:chunk_end]
                
                # Convert chunk to bytes
                chunk_io = io.BytesIO()
                chunk.export(chunk_io, format="wav")
                chunk_bytes = chunk_io.getvalue()
                
                chunks.append((chunk_index, chunk_bytes, chunk_start, chunk_end))
                
                # Move to next chunk
                start = end
                chunk_index += 1
                
            return chunks
            
        except Exception as e:
            st.error(f"Error splitting audio: {e}")
            # Fallback: return original audio as single chunk
            return [(0, audio_bytes, 0, 0)]

    def transcribe_chunk_openai(chunk_data):
        """
        Transcribe a single chunk using OpenAI
        
        Args:
            chunk_data: Tuple of (chunk_index, chunk_bytes, start_time, end_time)
        
        Returns:
            Dictionary with transcription result
        """
        chunk_index, chunk_bytes, start_time, end_time = chunk_data
        
        try:
            # Create temporary file for this chunk
            temp_filename = f"temp_chunk_{chunk_index}_{int(time.time())}.wav"
            
            with open(temp_filename, "wb") as temp_file:
                temp_file.write(chunk_bytes)
            
            # Call your existing OpenAI transcription function
            transcript = transcribe_with_openai_chunked(chunk_bytes, temp_filename)
            
            # Clean up temp file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            
            if transcript and len(transcript.strip()) > 5:
                return {
                    "success": True,
                    "chunk_index": chunk_index,
                    "transcript": transcript.strip(),
                    "start_time": start_time,
                    "end_time": end_time,
                    "method": "OpenAI"
                }
            else:
                return {
                    "success": False,
                    "chunk_index": chunk_index,
                    "error": "Empty or invalid transcript",
                    "start_time": start_time,
                    "end_time": end_time
                }
                
        except Exception as e:
            return {
                "success": False,
                "chunk_index": chunk_index,
                "error": str(e),
                "start_time": start_time,
                "end_time": end_time
            }

    def transcribe_chunk_gemini(chunk_data):
        """
        Transcribe a single chunk using Gemini (fallback)
        
        Args:
            chunk_data: Tuple of (chunk_index, chunk_bytes, start_time, end_time)
        
        Returns:
            Dictionary with transcription result
        """
        chunk_index, chunk_bytes, start_time, end_time = chunk_data
        
        try:
            # Create temporary file for this chunk
            temp_filename = f"temp_chunk_gemini_{chunk_index}_{int(time.time())}.wav"
            
            with open(temp_filename, "wb") as temp_file:
                temp_file.write(chunk_bytes)
            
            # Call your existing Gemini transcription function
            transcript = transcribe_audio_gemini(temp_filename)
            
            # Clean up temp file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            
            if transcript and len(transcript.strip()) > 5:
                return {
                    "success": True,
                    "chunk_index": chunk_index,
                    "transcript": transcript.strip(),
                    "start_time": start_time,
                    "end_time": end_time,
                    "method": "Gemini"
                }
            else:
                return {
                    "success": False,
                    "chunk_index": chunk_index,
                    "error": "Empty or invalid transcript",
                    "start_time": start_time,
                    "end_time": end_time
                }
                
        except Exception as e:
            return {
                "success": False,
                "chunk_index": chunk_index,
                "error": str(e),
                "start_time": start_time,
                "end_time": end_time
            }

    def transcribe_with_parallel_chunks(audio_file_bytes, file_name, max_workers=4):
        """
        Transcribe audio using parallel chunk processing
        
        Args:
            audio_file_bytes: Raw audio file bytes
            file_name: Original audio file name
            max_workers: Maximum number of parallel workers
        
        Returns:
            Dictionary with transcription result
        """
        
        st.info("🔧 Splitting audio into chunks for parallel processing...")
        
        # Split audio into chunks
        chunks = split_audio_into_chunks(audio_file_bytes)
        
        st.info(f"📋 Created {len(chunks)} chunks for parallel transcription")
        
        # Progress tracking for chunks
        chunk_progress = st.progress(0)
        chunk_status = st.empty()
        
        # Parallel transcription with OpenAI first
        st.info("🚀 Starting parallel transcription with OpenAI...")
        
        successful_chunks = []
        failed_chunks = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all OpenAI transcription tasks
            future_to_chunk = {
                executor.submit(transcribe_chunk_openai, chunk): chunk 
                for chunk in chunks
            }
            
            completed = 0
            total_chunks = len(chunks)
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                
                try:
                    result = future.result()
                    
                    if result["success"]:
                        successful_chunks.append(result)
                        chunk_status.success(f"✅ Chunk {result['chunk_index'] + 1}/{total_chunks} completed with {result['method']}")
                    else:
                        failed_chunks.append(chunk)
                        chunk_status.warning(f"⚠️ Chunk {result['chunk_index'] + 1}/{total_chunks} failed with OpenAI, will retry with Gemini")
                    
                except Exception as exc:
                    failed_chunks.append(chunk)
                    chunk_status.error(f'❌ Chunk {chunk[0] + 1}/{total_chunks} generated exception: {exc}')
                
                completed += 1
                chunk_progress.progress(completed / total_chunks * 0.7)  # 70% for OpenAI phase
        
        # Retry failed chunks with Gemini
        if failed_chunks:
            st.info(f"🔄 Retrying {len(failed_chunks)} failed chunks with Gemini...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit failed chunks to Gemini
                future_to_chunk = {
                    executor.submit(transcribe_chunk_gemini, chunk): chunk 
                    for chunk in failed_chunks
                }
                
                gemini_completed = 0
                
                # Process Gemini results
                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    
                    try:
                        result = future.result()
                        
                        if result["success"]:
                            successful_chunks.append(result)
                            chunk_status.success(f"✅ Chunk {result['chunk_index'] + 1}/{total_chunks} completed with Gemini (retry)")
                        else:
                            chunk_status.error(f"❌ Chunk {result['chunk_index'] + 1}/{total_chunks} failed with both OpenAI and Gemini")
                    
                    except Exception as exc:
                        chunk_status.error(f'❌ Chunk {chunk[0] + 1}/{total_chunks} failed completely: {exc}')
                    
                    gemini_completed += 1
                    current_progress = 0.7 + (gemini_completed / len(failed_chunks) * 0.3)
                    chunk_progress.progress(current_progress)
        
        chunk_progress.progress(1.0)
        
        # Sort chunks by index to maintain order
        successful_chunks.sort(key=lambda x: x['chunk_index'])
        
        if not successful_chunks:
            return {
                "success": False,
                "error": "All chunks failed transcription",
                "method": "Parallel Processing (All Failed)"
            }
        
        # Combine transcripts in order
        st.info("🔗 Combining transcripts in correct order...")
        
        combined_transcript = ""
        transcription_methods = []
        
        for chunk_result in successful_chunks:
            # Add space between chunks if needed
            if combined_transcript and not combined_transcript.endswith(" "):
                combined_transcript += " "
            
            combined_transcript += chunk_result["transcript"]
            transcription_methods.append(chunk_result["method"])
        
        # Clean up combined transcript
        combined_transcript = " ".join(combined_transcript.split())  # Remove extra whitespaces
        
        # Determine primary method used
        openai_count = transcription_methods.count("OpenAI")
        gemini_count = transcription_methods.count("Gemini")
        
        if openai_count > gemini_count:
            primary_method = f"OpenAI Parallel ({openai_count}/{len(successful_chunks)} chunks)"
        elif gemini_count > openai_count:
            primary_method = f"Gemini Parallel ({gemini_count}/{len(successful_chunks)} chunks)"
        else:
            primary_method = f"Mixed Parallel (OpenAI: {openai_count}, Gemini: {gemini_count})"
        
        success_rate = len(successful_chunks) / len(chunks) * 100
        
        return {
            "success": True,
            "transcript": combined_transcript,
            "method": primary_method,
            "chunks_processed": len(successful_chunks),
            "total_chunks": len(chunks),
            "success_rate": success_rate,
            "details": {
                "openai_chunks": openai_count,
                "gemini_chunks": gemini_count,
                "failed_chunks": len(chunks) - len(successful_chunks)
            }
        }

    def predictor_direct(transcript, registration_id, competencies):
        """
        Modified predictor that works directly with transcript without database dependency
        """
        try:
            # Your existing prediction logic here, but using the passed transcript directly
            # instead of querying from database
            
            # Example implementation:
            prediction_prompt = f"""
            Analyze this audio transcript for competencies: {competencies}
            
            Transcript:
            {transcript}
            
            Registration ID: {registration_id}
            
            Please provide competency assessment...
            """
            
            # Call your AI model for prediction
            # Replace this with your actual prediction logic
            result = {
                "success": True,
                "predictions": "Your prediction results here",
                "competencies_analyzed": competencies
            }
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
# ============================ END OF MAINTENANCE TRANSCRIBE =====================================

    if st.button("Upload, Transcribe dan Prediksi", key="SimpanTranscribeFullParallel"):
        if audio_file is not None:
            # Timer
            start_time = time.time()
            
            # Get audio data once
            audio_file_bytes = audio_file.getvalue()
            file_name = audio_file.name
            file_extension = file_name.split('.')[-1].lower()
            
            # Progress tracking
            progress_bar = st.progress(0)
            
            # === START PARALLEL PROCESSES IMMEDIATELY ===
            #st.info("🚀 Starting PARALLEL processing: Transcription + S3 Upload...")
            progress_bar.progress(5)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as main_executor:
                
                # PROCESS 1: TRANSCRIPTION (Priority)
                transcription_future = main_executor.submit(
                    transcribe_with_parallel_chunks,
                    audio_file_bytes, 
                    file_name, 
                    4  # max_workers
                )
                
                # PROCESS 2: S3 UPLOAD (Parallel with transcription)
                def upload_to_s3_parallel():
                    try:
                        s3_client = boto3.client('s3',
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    endpoint_url=endpoint_url)
                        
                        s3_client.upload_fileobj(io.BytesIO(audio_file_bytes), 'rpi-ta', file_name)
                        return {"success": True, "message": f"S3 upload completed"}
                    except Exception as e:
                        return {"success": False, "error": str(e)}
                
                s3_future = main_executor.submit(upload_to_s3_parallel)
                
                #st.success("✅ Parallel processes STARTED")
                progress_bar.progress(15)

                st.success(f"Step 1/8: File {file_name} berhasil terupload.")
                
                # === WAIT FOR TRANSCRIPTION (Priority) ===
                #st.info("⚡ Waiting for transcription...")
                transcription_result = transcription_future.result()
                
                if not transcription_result["success"]:
                    st.error(f"❌ Transcription failed: {transcription_result['error']}")
                    st.stop()
                
                transcript = transcription_result["transcript"]
                transcription_method = transcription_result["method"]
                
                # Show transcription stats
                stats = transcription_result.get("details", {})
                # st.success(f"""
                # ⚡ TRANSCRIPTION COMPLETED!
                
                # 📊 **Stats:**
                # - Method: {transcription_method}
                # - Chunks: {transcription_result.get('chunks_processed', 0)}/{transcription_result.get('total_chunks', 0)}
                # - Success Rate: {transcription_result.get('success_rate', 0):.1f}%
                # - OpenAI: {stats.get('openai_chunks', 0)}
                # - Gemini: {stats.get('gemini_chunks', 0)}
                # """)
                
                progress_bar.progress(40)
                st.success("Step 2/8: Audio berhasil dikirim untuk transkripsi.")
                
                # === START SPEAKER SEPARATION IN BACKGROUND ===
                def process_speaker_separation_background():
                    try:
                        # Debug: Log function start
                        #print("🔍 DEBUG: process_speaker_separation_background() STARTED")
                        
                        # Debug: Check variables in thread scope
                        #print(f"🔍 DEBUG: selected_option_num_speaker = {selected_option_num_speaker} (type: {type(selected_option_num_speaker)})")
                        #print(f"🔍 DEBUG: transcript length = {len(transcript) if 'transcript' in globals() else 'NOT FOUND'}")
                        
                        # FIXED: Ensure integer conversion and proper variable access
                        try:
                            num_speakers = int(selected_option_num_speaker) if selected_option_num_speaker else 2
                        except (ValueError, TypeError):
                            print(f"🔍 DEBUG: Failed to convert {selected_option_num_speaker} to int, defaulting to 2")
                            num_speakers = 2
                        
                        #print(f"🔍 DEBUG: Converted num_speakers = {num_speakers}")
                        print(f"🔍 DEBUG: Final num_speakers = {num_speakers}")
                        
                        if num_speakers > 1:
                            #print("🔍 DEBUG: Multi-speaker condition TRUE - proceeding with separation")
                            #st.info("🎭 Background: Starting speaker separation...")
                            
                            # FIXED: Call separate_speakers_parallel with proper error handling
                            #print("🔍 DEBUG: About to call separate_speakers_parallel...")
                            
                            separated_transcript = separate_speakers_parallel(
                                transcript, 
                                2,
                                max_workers=3
                            )
                            
                            #print(f"🔍 DEBUG: separate_speakers_parallel completed. Result type: {type(separated_transcript)}")
                            
                            # Debug: Show raw separated result
                            if separated_transcript:
                                print(f"🔍 DEBUG: Separated transcript length: {len(separated_transcript)}")
                                print(f"🔍 DEBUG: Separated transcript content preview:")
                                print(separated_transcript[:500])
                                
                                # PERBAIKAN 4: Check jika hasil hanya 1 line tanpa separation
                                lines = separated_transcript.strip().split('\n')
                                speaker_lines = [line for line in lines if '**' in line or ':' in line]
                                print(f"🔍 DEBUG: Found {len(speaker_lines)} speaker lines out of {len(lines)} total lines")
                                
                                if len(speaker_lines) < 2:
                                    print("🔍 DEBUG: WARNING - Very few speaker lines detected!")
                                    print("🔍 DEBUG: This suggests speaker separation failed")
                                    # Tampilkan isi untuk debugging
                                    st.warning("⚠️ DEBUG: Speaker separation mungkin gagal - hanya sedikit baris speaker terdeteksi")
                                    st.code(separated_transcript[:1000])  # Show first 1000 chars for debugging
                            else:
                                print("🔍 DEBUG: separated_transcript is empty/None")
                                return {"success": False, "error": "separated_transcript is empty"}
                            
                            # Process to DataFrame using existing function
                            print("🔍 DEBUG: Processing to DataFrame...")
                            df_separated = process_gpt_response_to_dataframe(separated_transcript)
                            
                            if df_separated is not None and not df_separated.empty:
                                #print(f"🔍 DEBUG: DataFrame created successfully. Shape: {df_separated.shape}")
                                
                                # Debug info
                                print(f"🔍 DEBUG: DataFrame shape: {df_separated.shape}")
                                print(f"🔍 DEBUG: DataFrame columns: {df_separated.columns.tolist()}")
                                print(f"🔍 DEBUG: DataFrame dtypes: {df_separated.dtypes.to_dict()}")
                                print(f"🔍 DEBUG: Sample data:")
                                # st.dataframe(df_separated.head(3))
                                
                                # Ensure required columns exist
                                required_columns = ['text', 'speaker']
                                missing_cols = [col for col in required_columns if col not in df_separated.columns]
                                
                                if missing_cols:
                                    # print(f"🔍 DEBUG: Missing columns: {missing_cols}")
                                    st.warning(f"⚠️ Missing columns in DataFrame: {missing_cols}")
                                    # Add missing columns with defaults
                                    for col in missing_cols:
                                        if col == 'text':
                                            df_separated['text'] = separated_transcript
                                        elif col == 'speaker':
                                            df_separated['speaker'] = 'Kandidat'
                                
                                # Ensure start_time and end_time columns exist with proper types
                                if 'start_time' not in df_separated.columns:
                                    df_separated['start_time'] = range(len(df_separated))
                                if 'end_time' not in df_separated.columns:
                                    df_separated['end_time'] = range(len(df_separated))
                                
                                # Fix data types
                                df_separated['start_time'] = pd.to_numeric(df_separated['start_time'], errors='coerce').fillna(0).astype(int)
                                df_separated['end_time'] = pd.to_numeric(df_separated['end_time'], errors='coerce').fillna(0).astype(int)
                                
                                # FIXED: Enhanced ordering for better sequence
                                def sort_by_text_position_enhanced_fixed(df_separated, original_transcript):
                                    try:
                                        def get_position_score(text):
                                            try:
                                                # Ensure text is string
                                                if not isinstance(text, str):
                                                    text = str(text)
                                                
                                                # Take first 10 words for matching
                                                clean_text = ' '.join(text.strip().split()[:10]).lower()
                                                clean_transcript = original_transcript.lower()
                                                
                                                # Direct search
                                                pos = clean_transcript.find(clean_text)
                                                if pos != -1:
                                                    return pos  # Return as int directly
                                                
                                                # Word by word search as fallback
                                                words = clean_text.split()
                                                for i, word in enumerate(words):
                                                    if len(word) > 3:
                                                        pos = clean_transcript.find(word)
                                                        if pos != -1:
                                                            return pos + (i * 100)  # Return as int directly
                                                
                                                return 999999  # Large number for not found
                                            except Exception as e:
                                                print(f"🔍 DEBUG: Error in get_position_score: {e}")
                                                return 999999
                                        
                                        # Apply position scoring
                                        df_separated = df_separated.copy()
                                        df_separated['position'] = df_separated['text'].apply(get_position_score)
                                        
                                        # Debug position values
                                        print(f"🔍 DEBUG: Position values: {df_separated['position'].tolist()}")
                                        print(f"🔍 DEBUG: Position values: {df_separated['position'].tolist()}")
                                        
                                        # Ensure all are numeric
                                        df_separated['position'] = pd.to_numeric(df_separated['position'], errors='coerce').fillna(999999).astype(int)
                                        
                                        # Sort and clean
                                        df_sorted = df_separated.sort_values('position').reset_index(drop=True)
                                        df_sorted = df_sorted.drop('position', axis=1)
                                        return df_sorted
                                        
                                    except Exception as e:
                                        print(f"🔍 DEBUG: Error in sort_by_text_position_enhanced_fixed: {e}")
                                        st.error(f"🔍 DEBUG: Sorting error: {e}")
                                        return df_separated  # Return unsorted if sorting fails
                                
                                print("🔍 DEBUG: Applying enhanced sorting...")
                                df_separated = sort_by_text_position_enhanced_fixed(df_separated, transcript)
                                
                                # Final validation - ensure speaker column has proper values
                                if 'speaker' in df_separated.columns:
                                    # Check if speakers are properly identified
                                    unique_speakers = df_separated['speaker'].unique()
                                    print(f"🔍 DEBUG: Unique speakers found: {unique_speakers}")
                                    print(f"🔍 DEBUG: Unique speakers found: {unique_speakers}")
                                    
                                    # If no proper speaker separation happened, warn user
                                    if len(unique_speakers) == 1 and unique_speakers[0] == 'Kandidat':
                                        print("🔍 DEBUG: Warning - all entries marked as 'Kandidat'")
                                        st.warning("⚠️ Speaker separation may not have worked - all entries marked as 'Kandidat'")
                                
                                print("🔍 DEBUG: Returning successful result")
                                return {
                                    "success": True,
                                    "df_separated": df_separated,
                                    "method": "Parallel Separation with Enhanced Ordering (Fixed)",
                                    "entries": len(df_separated)
                                }
                            else:
                                print("🔍 DEBUG: DataFrame processing failed")
                                return {"success": False, "error": "Failed to process separated transcript"}
                        else:
                            # Single speaker
                            print(f"🔍 DEBUG: Single speaker mode (num_speakers = {num_speakers})")
                            single_df = pd.DataFrame({
                                'speaker': ['Kandidat'],
                                'text': [transcript],
                                'start_time': [0],
                                'end_time': [0]
                            })
                            return {
                                "success": True,
                                "df_separated": single_df,
                                "method": "Single Speaker",
                                "entries": 1
                            }
                            
                    except Exception as e:
                        print(f"🔍 DEBUG: Exception in process_speaker_separation_background: {e}")
                        st.error(f"🔍 DEBUG: Exception in process_speaker_separation_background: {e}")
                        import traceback
                        error_traceback = traceback.format_exc()
                        print(f"🔍 DEBUG: Traceback:\n{error_traceback}")
                        st.code(error_traceback)
                        return {"success": False, "error": str(e), "traceback": error_traceback}
                
                # Start separation in background
                separation_future = main_executor.submit(process_speaker_separation_background)
                # st.success("🔄 Background: Speaker separation STARTED")
                
                # === IMMEDIATE PREDICTION (No Database Insert) ===
                # st.info("🎯 PRIORITY: Running IMMEDIATE prediction...")
                progress_bar.progress(50)
                st.success("Step 3/8: Pembicara berhasil ditambahkan.")
                
                try:
                    # Show transcript preview
                    print("🔍 Transcript Preview:")
                    preview_text = transcript[:500] + "..." if len(transcript) > 500 else transcript
                    # st.text_area("Transcript", preview_text, height=150)
                    
                    # Create temporary minimal database entry ONLY for prediction
                    tz = pytz.timezone('Asia/Jakarta')
                    conn = create_db_connection()
                    cursor = conn.cursor()
                    
                    selected_id_product = int(selected_product['id_product'].iloc[0])
                    
                    insert_query = """
                    INSERT INTO txtan_audio (registration_id, date, num_speakers, id_product, id_level_set, kode_assessor, audio_file_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    data = (
                        id_input_id_kandidat,
                        datetime.now(tz),
                        1,  # Temporary single speaker for prediction
                        selected_id_product,
                        selected_option_level_set,
                        id_input_kode_assessor,
                        file_name
                    )
                    cursor.execute(insert_query, data)
                    conn.commit()
                    temp_id_audio = cursor.lastrowid
                    
                    # Single transcript entry for prediction
                    insert_query = """
                    INSERT INTO txtan_separator (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        temp_id_audio,
                        id_input_id_kandidat,
                        transcript,
                        'Kandidat',
                        0,
                        0
                    ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    progress_bar.progress(60)
                    st.success("Step 4/8: Mohon tunggu, proses prediksi berlangsung.....")
                    
                    # RUN PREDICTION with existing function
                    # st.info("🤖 Running AI Prediction...")
                    predictor(id_input_id_kandidat, dropdown_options_predict_competency)
                    
                    progress_bar.progress(75)
                    # st.success("⚡ PREDICTION COMPLETED!")
                    
                    # Show prediction ready notification
                    st.success("Step 5/8: AI selesai menganalisis, hasil prediksi siap!")
                    st.info("🎉 **PREDIKSI SUDAH SIAP!** Anda sudah bisa melihat hasil prediksi dengan aman di halaman web baru.")
                    st.warning("⚠️ **PENTING:** Jangan tutup halaman ini sampai semua proses selesai (Step 8/8)")
                    
                    prediction_time = time.time()
                    
                except Exception as e:
                    st.error(f"❌ Prediction failed: {e}")
                    import traceback
                    st.error("Error traceback:")
                    st.code(traceback.format_exc())
                    st.stop()
                
                # === BACKGROUND COMPLETION HANDLING ===
                # st.info("🔄 Processing background tasks completion...")
                progress_bar.progress(85)
                st.success("Step 6/8: Melakukan finalisasi.")
                st.info("⏳ Sedang menyelesaikan proses terakhir - mohon tidak menutup halaman ini...")
                
                # Check S3 status
                # try:
                #     s3_result = s3_future.result(timeout=5)
                #     if s3_result["success"]:
                #         # st.success(f"✅ S3 Upload: {s3_result['message']}")
                #     else:
                #         st.warning(f"⚠️ S3 Upload: {s3_result['error']}")
                # except concurrent.futures.TimeoutError:
                #     st.info("🔄 S3 upload continuing...")
                # except Exception as e:
                #     st.warning(f"⚠️ S3 error: {str(e)}")
                
                # Check separation status and save final results
                try:
                    separation_result = separation_future.result(timeout=120)
                    if separation_result["success"]:
                        # st.success(f"⚡ Speaker Separation: {separation_result['method']} - {separation_result['entries']} entries")
                        
                        # Save final separated results to database
                        # st.info("💾 Saving final separated results...")
                        
                        progress_bar.progress(95)
                        st.success("Step 7/8: Finalisasi data ke sistem...")
                        
                        try:
                            conn = create_db_connection()
                            cursor = conn.cursor()
                            
                            # Create final audio record with correct speaker count
                            insert_query = """
                            INSERT INTO txtan_audio (registration_id, date, num_speakers, id_product, id_level_set, kode_assessor, audio_file_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            data = (
                                id_input_id_kandidat,
                                datetime.now(tz),
                                selected_option_num_speaker,
                                selected_id_product,
                                selected_option_level_set,
                                id_input_kode_assessor,
                                file_name
                            )
                            cursor.execute(insert_query, data)
                            conn.commit()
                            final_id_audio = cursor.lastrowid

                            delete_old_query = "DELETE FROM txtan_separator WHERE registration_id = %s"
                            cursor.execute(delete_old_query, (id_input_id_kandidat,))
                            conn.commit()
                            
                            # Batch insert separated entries
                            df_separated = separation_result["df_separated"]
                            batch_data = []
                            for index, row in df_separated.iterrows():
                                batch_data.append((
                                    final_id_audio,
                                    id_input_id_kandidat,
                                    row['text'],
                                    row['speaker'],
                                    row.get('start_time', index),
                                    row.get('end_time', index)
                                ))
                            
                            insert_query = """
                            INSERT INTO txtan_separator (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.executemany(insert_query, batch_data)
                            conn.commit()
                            
                            cursor.close()
                            conn.close()
                            
                            # st.success(f"✅ Database: Final results saved - {len(batch_data)} entries")
                            
                        except Exception as db_e:
                            st.error(f"❌ Database save error: {str(db_e)}")
                            
                    else:
                        st.warning(f"⚠️ Speaker separation: {separation_result['error']}")
                        
                except concurrent.futures.TimeoutError:
                    print("🔄 Speaker separation still processing...")
                except Exception as e:
                    st.warning(f"⚠️ Separation error: {str(e)}")
            
            progress_bar.progress(100)
            
            # Calculate times
            prediction_total_time = prediction_time - start_time
            total_time = time.time() - start_time
            
            pred_minutes, pred_seconds = divmod(int(prediction_total_time), 60)
            total_minutes, total_seconds = divmod(int(total_time), 60)
            
            # Final summary
            st.success("Step 8/8: ✅ Semua proses selesai!.")
            st.success(f"🎉 Proses berhasil diselesaikan dalam {total_minutes} menit {total_seconds} detik")
            st.info("💡 **Sekarang aman untuk atau menutup tab ini**")
        
        else:
            st.error("❌ Please upload an audio file first!")

########################TAB 2
with tab2:
    # Restart button for audio to transcript process
    def restart_audio_to_transcript_fully_optimized(registration_id):
        """
        Fully optimized restart with parallel transcription AND parallel speaker separation
        """
        try:
            conn = create_db_connection()
            cursor = conn.cursor()
            
            print(f"DEBUG: Starting FULLY optimized restart for registration_id: {registration_id}")
            
            # [Previous database cleanup code remains the same...]
            
            # Check what data exists first
            check_audio_query = "SELECT COUNT(*) FROM txtan_audio WHERE registration_id = %s"
            cursor.execute(check_audio_query, (registration_id,))
            audio_count = cursor.fetchone()[0]
            
            check_separator_query = "SELECT COUNT(*) FROM txtan_separator WHERE registration_id = %s"
            cursor.execute(check_separator_query, (registration_id,))
            separator_count = cursor.fetchone()[0]
            
            # Delete existing separator data
            delete_separator_query = "DELETE FROM txtan_separator WHERE registration_id = %s"
            cursor.execute(delete_separator_query, (registration_id,))
            deleted_rows = cursor.rowcount
            conn.commit()
            
            # Check transcriptions
            transcriptions = get_transcriptions(registration_id)
            
            if transcriptions:
                # Skenario 2: Process from existing transcriptions
                process_transcriptions(registration_id)
                # st.success("Proses audio ke transkrip berhasil di-restart dari txtan_transkrip!")
            else:
                # Skenario 1: Process from S3 audio
                audio_query = """
                SELECT audio_file_name, num_speakers, id_product, id_level_set, kode_assessor, id_audio
                FROM txtan_audio WHERE registration_id = %s
                """
                cursor.execute(audio_query, (registration_id,))
                audio_result = cursor.fetchone()
                
                if audio_result:
                    audio_file_name, num_speakers, id_product, id_level_set, kode_assessor, id_audio = audio_result
                    
                    # Download from S3
                    s3_client = boto3.client('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                endpoint_url=endpoint_url)
                    
                    try:
                        audio_obj = s3_client.get_object(Bucket='rpi-ta', Key=audio_file_name)
                        audio_bytes = audio_obj['Body'].read()
                        
                        # === STEP 1: PARALLEL TRANSCRIPTION ===
                        # st.info("🚀 Step 1/4: PARALLEL Transcription...")
                        transcript = transcribe_with_openai_chunked_parallel(
                            audio_bytes, audio_file_name, max_workers=4
                        )
                        
                        if not transcript:
                            st.error("❌ Transcription failed")
                            return
                        
                        # st.success("⚡ Step 1/4: Parallel transcription completed!")
                        
                        # === STEP 2: PARALLEL SPEAKER SEPARATION ===
                        # st.info("🎭 Step 2/4: PARALLEL Speaker Separation...")
                        transcript_text = transcript.text if hasattr(transcript, 'text') else str(transcript)
                        
                        # Use parallel speaker separation
                        separated_transcript = separate_speakers_parallel(
                            transcript_text, 
                            2, 
                            max_workers=3  # Lower for GPT API limits
                        )
                        
                        # st.success("⚡ Step 2/4: Parallel speaker separation completed!")
                        
                        # === STEP 3: PROCESS TO DATAFRAME ===
                        # st.info("🔄 Step 3/4: Processing to database format...")
                        print("DEBUG separated_transcript:", separated_transcript)
                        df_result = process_gpt_response_to_dataframe(separated_transcript)

                        if isinstance(df_result, str):
                            
                            # Parse the string format manually
                            lines = df_result.strip().split('\n')[1:]  # Skip header
                            data = {'text': [], 'speaker': []}
                            
                            for line in lines:
                                if line.strip():
                                    # Use regex to extract: number + text + speaker
                                    match = re.match(r'(\d+)\s+(.+?)\s+(Kandidat|Assessor)$', line.strip())
                                    if match:
                                        index, text, speaker = match.groups()
                                        data['text'].append(text.strip())
                                        data['speaker'].append(speaker.strip())
                                    else:
                                        # Fallback: split by last word (assume it's speaker)
                                        parts = line.strip().split()
                                        if len(parts) >= 3:  # At least index + some text + speaker
                                            speaker = parts[-1]
                                            text = ' '.join(parts[1:-1])  # Everything between index and speaker
                                            if speaker in ['Kandidat', 'Assessor']:
                                                data['text'].append(text.strip())
                                                data['speaker'].append(speaker.strip())
                            
                            df = pd.DataFrame(data)
                            st.write(f"Parsed {len(df)} rows from string")
                        else:
                            df = df_result
                        
                        if df is not None and not df.empty:
                            # st.success("✅ Step 3/4: Database format ready!")
                            
                            # Debug DataFrame structure
                            print(f"🔍 DataFrame shape: {df.shape}")
                            print(f"🔍 DataFrame columns: {list(df.columns)}")
                            
                            # === STEP 4: BATCH DATABASE INSERT ===
                            # st.info("🚀 Step 4/4: BATCH database insert...")
                            
                            batch_data = []
                            print(f"DEBUG batch data before: {batch_data}")
                            
                            # Debug each row processing
                            for i in range(len(df)):
                                text_val = df.iloc[i]['text']
                                speaker_val = df.iloc[i]['speaker']
                                
                                print(f"🔍 Row {i}: text='{str(text_val)[:30]}...', speaker='{speaker_val}'")
                                
                                batch_data.append((
                                    id_audio, 
                                    registration_id, 
                                    text_val, 
                                    speaker_val, 
                                    0, 
                                    0
                                ))
                                
                                print(f"🔍 Batch data now has {len(batch_data)} items")
                            
                            print(f"DEBUG batch data after loop: {len(batch_data)} items")
                            print(f"🔍 Sample batch item: {batch_data[0] if batch_data else 'No items'}")
                            
                            if len(batch_data) > 0:
                                try:
                                    # Properly close existing cursor
                                    try:
                                        cursor.close()
                                    except:
                                        pass
                                    
                                    # Clear any pending operations
                                    try:
                                        conn.rollback()  # Clear any pending transactions
                                    except:
                                        pass
                                    
                                    # Create fresh cursor
                                    cursor = conn.cursor()
                                    
                                    insert_query = """
                                    INSERT INTO txtan_separator (id_transkrip, registration_id, revisi_transkrip, revisi_speaker, revisi_start_section, revisi_end_section)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    """
                                    
                                    print("🔍 Executing database insert...")
                                    cursor.executemany(insert_query, batch_data)
                                    conn.commit()
                                    cursor.close()
                                    
                                    # st.success(f"⚡ Step 4/4: Batch inserted {len(batch_data)} rows!")
                                    # st.success("🎉 FULLY OPTIMIZED restart completed successfully!")
                                    
                                except Exception as db_error:
                                    # st.error(f"❌ Database error: {db_error}")
                                    print(f"🔍 Error details: {str(db_error)}")
                                    try:
                                        cursor.close()
                                    except:
                                        pass
                            else:
                                st.error("❌ batch_data is empty after processing!")
                            
                        else:
                            st.error("❌ Failed to process transcript to dataframe")

                    except Exception as e:
                        st.error(f"❌ Error in optimized process: {e}")
                else:
                    st.error("❌ No audio data found")
                    
        except Exception as e:
            st.error(f"❌ Error during fully optimized restart: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    # Add restart button
    if id_input_id_kandidat:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Hasil Transkrip")
        with col2:
            if st.button("🔄 Restart Audio → Transkrip", key="restart_audio_transcript", help="Restart proses dari audio ke transkrip dengan pemisahan speaker"):
                restart_audio_to_transcript_fully_optimized(id_input_id_kandidat)
                st.rerun()

    with st.container():
        def get_transkrip_data(registration_id):
            conn = create_db_connection()
            if conn is None:
                st.error("Database connection not available.")
                return pd.DataFrame(columns=["Start", "End", "Transkrip", "Speaker"])

            try:
                cursor = conn.cursor()
                query = """
                SELECT revisi_start_section AS 'Start', revisi_end_section AS 'End', revisi_transkrip AS 'Transkrip', revisi_speaker AS 'Speaker'
                FROM txtan_separator
                WHERE registration_id = %s
                """
                cursor.execute(query, (registration_id,))
                result = cursor.fetchall()
                cursor.close()
                conn.close()

                if result:
                    df = pd.DataFrame(result, columns=["Start", "End", "Transkrip", "Speaker"]) #start dan end masihh dalam sec
                    return df
                else:
                    return pd.DataFrame(columns=["Start", "End", "Transkrip", "Speaker"])

            except mysql.connector.Error as e:
                st.error(f"Error fetching transcription data: {e}")
                return pd.DataFrame(columns=["Start", "End", "Transkrip", "Speaker"])
            finally:
                if conn.is_connected():
                    conn.close()
        
        if id_input_id_kandidat:
            df_transkrip = get_transkrip_data(id_input_id_kandidat)
            df_transkrip_reset = df_transkrip.reset_index(drop=True)
            table_html = df_transkrip_reset.to_html(index=False, escape=False)
            st.markdown("""
                <style>
                table {
                    width: 100%;
                    border-collapse: collapse;
                }
                th, td {
                    text-align: left;
                    vertical-align: top;
                    padding: 8px;
                    border: 1px solid #ddd;
                    word-wrap: break-word;
                    white-space: pre-wrap;
                }
                th {
                    background-color: #00;
                }
                </style>
            """, unsafe_allow_html=True)
            st.markdown(table_html, unsafe_allow_html=True)
        else:
            st.warning("ID Kandidat tidak ditemukan/kosong")

########################TAB 3
with tab3:
    # Restart button for transcript to competency prediction process
    def restart_transcript_to_prediction(registration_id):
        """Restart the transcript to competency prediction process for a given registration ID"""
        try:
            # Clear existing competency result data
            conn = create_db_connection()
            cursor = conn.cursor()
            
            # Delete existing competency result data for this registration_id
            delete_result_query = """
            DELETE FROM txtan_competency_result WHERE registration_id = %s
            """
            cursor.execute(delete_result_query, (registration_id,))
            conn.commit()
            
            # Clear session state for original results
            if 'original_results' in st.session_state:
                del st.session_state['original_results']
            
            # Run prediction again
            predictor(registration_id, dropdown_options_predict_competency)
            #st.success("Proses transkrip ke prediksi kompetensi berhasil di-restart!")
                
        except Exception as e:
            st.error(f"Error saat restart proses transkrip ke prediksi: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    # Add restart button
    if id_input_id_kandidat:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Hasil Prediksi Kompetensi")
        with col2:
            if st.button("🔄 Restart Transkrip → Prediksi", key="restart_transcript_prediction", help="Restart proses dari transkrip ke prediksi kompetensi"):
                restart_transcript_to_prediction(id_input_id_kandidat)
                st.rerun()
    
    with st.container(border=True):
        st.write("Pilihan 'kosong' ada bisa dipilih jika dirasa memang tidak muncul di Assessor")
        st.write("Dropdown kompetensi dan level kompetensi **di sidebar** tidak akan mengubah pilihan level di bagian ini")

    with st.container():
        def get_level_set_from_audio_table(registration_id):
            query = """
            SELECT a.id_level_set, lvl.name_level AS 'NAMA LEVEL'
            FROM txtan_audio a
            JOIN pito_level lvl ON a.id_level_set = lvl.id_level_set
            WHERE a.registration_id = %s
            """
            conn = create_db_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, (registration_id,))
                result = cursor.fetchone()
                cursor.fetchall()
                return result if result else (None, None)
            except Exception as e:
                print(f"Error fetching level set: {e}")
                return None, None
            finally:
                cursor.close()
                conn.close()
        
        def get_result_data(registration_id):
            query = """
            SELECT competency, level, reason
            FROM txtan_competency_result
            WHERE registration_id = %s
            """
            conn = create_db_connection()
            cursor = conn.cursor()
            cursor.execute(query, (registration_id,))
            result = cursor.fetchall()

            cursor.close()
        
            if result:
                df = pd.DataFrame(result, columns=["competency", "level", "reason"])
                return df
            else:
                return pd.DataFrame(columns=["competency", "level", "reason"])

        def save_so_to_db(data_to_save):
            query = """
            INSERT INTO txtan_competency_result (registration_id, competency, level, reason, so_level, so_reason)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor = conn.cursor()
            cursor.executemany(query, data_to_save)
            conn.commit()
            cursor.close()
  
        def update_single_entry_db(conn, competency, level, reason, so_level, so_reason, registration_id):
            try:
                cursor = conn.cursor()
                
                so_level = so_level if so_level != '' else None
                so_reason = so_reason if so_reason != '' else None

                
                check_query = """
                SELECT COUNT(*) FROM txtan_competency_result
                WHERE registration_id = %s AND competency = %s AND level = %s AND reason = %s
                """
                cursor.execute(check_query, (registration_id, competency, level, reason))
                count = cursor.fetchone()[0]

                if count > 0:
                    update_query = """
                    UPDATE txtan_competency_result
                    SET so_level = %s, so_reason = %s
                    WHERE registration_id = %s AND competency = %s AND level = %s AND reason = %s
                    """
                    cursor.execute(update_query, (so_level, so_reason, registration_id, competency, level, reason))
                else:
                    
                    insert_query = """
                    INSERT INTO txtan_competency_result (registration_id, competency, level, reason, so_level, so_reason)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (registration_id, competency, level, reason, so_level, so_reason))

                conn.commit()

            except Exception as e:
                st.error(f"Error updating or inserting entry: {e}")

            finally:
                cursor.close()

        def get_all_so_values(registration_id):
            conn = create_db_connection()
            try:
                cursor = conn.cursor()
                query = """
                SELECT competency, so_level, so_reason
                FROM txtan_competency_result
                WHERE registration_id = %s
                """
                cursor.execute(query, (registration_id,))
                return cursor.fetchall() 
            except mysql.connector.Error as e:
                print(f"Database error: {e}")
                return []  
            finally:
                cursor.close()
                conn.close()

        if id_input_id_kandidat:
            df_result_prediction = get_result_data(id_input_id_kandidat)

            if 'original_results' not in st.session_state:
                st.session_state['original_results'] = df_result_prediction.copy()
            
            df_result_prediction = st.session_state['original_results']

            id_level_set_fix, nama_level = get_level_set_from_audio_table(id_input_id_kandidat)
            
            filtered_levels = df_pito_level[df_pito_level['id_level_set'] == id_level_set_fix]
            dropdown_options = filtered_levels['NAMA LEVEL'].tolist()
            dropdown_options.insert(0, '')

            so_values = get_all_so_values(id_input_id_kandidat)
            so_dict = {comp[0]: (comp[1], comp[2]) for comp in so_values} 

            for i, row in enumerate(df_result_prediction.itertuples()):
                st.markdown(f"##### {row.competency}")
                st.write(f"###### Level: {row.level}")
                st.write(f"###### Alasan muncul: {row.reason}")

                so_level_key = f"dropdown_{i}"
                so_reason_key = f"text_input_{i}"

                current_so_level_value, current_so_reason_value = so_dict.get(row.competency, ("", ""))

                if f"prev_so_level_{i}" not in st.session_state:
                    st.session_state[f"prev_so_level_{i}"] = current_so_level_value
                if f"prev_so_reason_{i}" not in st.session_state:
                    st.session_state[f"prev_so_reason_{i}"] = current_so_reason_value

                so_level = st.selectbox(
                    f"SO Level {row.competency}", 
                    dropdown_options, 
                    key=so_level_key,
                    index=dropdown_options.index(current_so_level_value) if current_so_level_value in dropdown_options else 0
                )

                so_reason = st.text_area(
                    f"Keterangan (opsional)", 
                    value=current_so_reason_value if current_so_reason_value else "",
                    key=f"so_reason_{row.competency}_{i}"
                )

                if (so_level != st.session_state[f"prev_so_level_{i}"]) or (so_reason != st.session_state[f"prev_so_reason_{i}"]):
                    update_single_entry_db(create_db_connection(), row.competency, row.level, row.reason, so_level, so_reason, id_input_id_kandidat)

                    st.session_state[f"prev_so_level_{i}"] = so_level
                    st.session_state[f"prev_so_reason_{i}"] = so_reason

                    update_success = True

                #st.success(f"Update berhasil untuk: {row.competency}") #ini masih salah
        else:
            st.warning("ID Kandidat tidak ditemukan/kosong")

########################TAB 4
with tab4:
    with st.container(border=True):
        st.write("Berikut adalah fitur dimana Anda bisa menambahkan set kompetensi, set level dan kode assessor baru ke sistem")

    subtab1, subtab2, subtab3 = st.tabs(["⚙️ <admin> Input Set Kompetensi", "⚙️ <admin> Input Set Level", "⚙️ <admin> Input Kode Assessor"])

    ########################SUBTAB 1
    with subtab1:
        with st.container(border=True):
            st.subheader("Menambahkan Set Kompetensi")
            st.write("""
            Berikut adalah fitur dimana Anda bisa menambahkan set level secara mandiri.Set level yang sudah diinput disini akan muncul di tab Parameter pada  pilihan set level. Cara menambahkan level dengan:\n
            1. Masukkan nama Set Kompetensi\n
            2. Masukkan nama Kompetensi 1 pada Input Kompetensi 1 juga deskripsinya (jika ada).\n
            3. Jika ada maka masukkan nama Level Kompetensi, Value dan Deskripsinya.\n
            4. Klik tambah level jika membutuhkan nama level baru (ada kemungkinan delay jadi jika tidak muncul bisa di klik lagi)\n
            5. Klik tambah kompetensi jika ingin menambahkan kompetensi 2, kompetensi 3 dan seterusnya.\n
            5. Lakukan cek kembali lalu jika sudah yakin klik Simpan Set Kompetensi.\n
            """)
        # Clean up old session state if it exists
        if 'competencies' in st.session_state:
            del st.session_state['competencies']
        if 'competency_level_inputs' in st.session_state:
            del st.session_state['competency_level_inputs']

        def save_competencies_to_db(id_product):
            conn = create_db_connection()
            cursor = conn.cursor()

            query_find_competency = """
                SELECT id_competency FROM pito_competency WHERE competency = %s
            """
            query_insert_competency = """
                INSERT INTO pito_competency (id_product, competency, description) 
                VALUES (%s, %s, %s)
            """
            query_find_level = """
                SELECT id_pito_competency_level FROM pito_competency_level 
                WHERE id_competency = %s AND level_value = %s
            """
            query_insert_level = """
                INSERT INTO pito_competency_level (level_value, level_name, level_description, id_competency) 
                VALUES (%s, %s, %s, %s)
            """

            for competency, description, levels in st.session_state['competencies']:
                cursor.execute(query_find_competency, (competency,))
                result = cursor.fetchone()

                if result:
                    id_competency = result[0]
                else:
                    cursor.execute(query_insert_competency, (id_product, competency, description))
                    conn.commit()
                    id_competency = cursor.lastrowid  

                for level in levels:
                    cursor.execute(query_find_level, (id_competency, level["value"]))
                    level_exists = cursor.fetchone()

                    if not level_exists:
                        cursor.execute(query_insert_level, (
                            level["value"],
                            level["name"],
                            level["description"],
                            id_competency
                        ))
                    else:
                        st.warning(f"Level Value '{level['value']}' sudah ada untuk kompetensi '{competency}' dan tidak akan ditambahkan lagi.")

            conn.commit()
            cursor.close()
            conn.close()

        def is_product_exists(product_name):
            conn = create_db_connection()
            cursor = conn.cursor()
            
            query_check = """
                SELECT COUNT(*) FROM pito_product WHERE name_product = %s
            """
            cursor.execute(query_check, (product_name,))
            exists = cursor.fetchone()[0] > 0
            
            cursor.close()
            conn.close()
            
            return exists

        

        # Initialize competency inputs in session state
        if 'competency_inputs' not in st.session_state:
            st.session_state['competency_inputs'] = [{"competency": "", "description": "", "levels": [{"name": "", "value": 0, "description": ""}]}]

        with st.container(border=True):
            input_name_product = st.text_input('Name Set Kompetensi', key='name_competency_set')
            
            # Display all competency inputs dynamically
            for comp_idx, competency_input in enumerate(st.session_state['competency_inputs']):
                with st.container(border=True):
                    col1, col2 = st.columns([5, 3], vertical_alignment="bottom")
                    with col1:
                        st.write(f"**Kompetensi {comp_idx + 1}**")
                    with col2:
                        if len(st.session_state['competency_inputs']) > 1:
                            if st.button(f"Delete Competency {comp_idx + 1}", key=f"delete_comp_{comp_idx}", icon="🗑️", use_container_width=True):
                                st.session_state['competency_inputs'].pop(comp_idx)
                                st.rerun()
                    
                    # Competency name and description
                    st.session_state['competency_inputs'][comp_idx]['competency'] = st.text_input(
                        f"Nama Kompetensi {comp_idx + 1}",
                        value=competency_input['competency'],
                        key=f"competency_name_{comp_idx}"
                    )
                    st.session_state['competency_inputs'][comp_idx]['description'] = st.text_area(
                        f"Deskripsi Kompetensi {comp_idx + 1}",
                        value=competency_input['description'],
                        key=f"competency_desc_{comp_idx}"
                    )
                    
                    # Levels for this competency
                    st.write(f"**Definisi Level untuk Kompetensi {comp_idx + 1}:**")
                    with st.container(border=True):
                        for level_idx, level_input in enumerate(competency_input['levels']):
                            col1, col2, col3 = st.columns([4, 2, 3], vertical_alignment="bottom")
                            with col1:
                                st.session_state['competency_inputs'][comp_idx]['levels'][level_idx]['name'] = st.text_input(
                                    f"Nama Level {level_idx + 1}",
                                    value=level_input['name'],
                                    key=f"comp_{comp_idx}_level_name_{level_idx}"
                                )
                            with col2:
                                st.session_state['competency_inputs'][comp_idx]['levels'][level_idx]['value'] = st.number_input(
                                    f"Value Level {level_idx + 1}",
                                    value=level_input['value'],
                                    step=1,
                                    key=f"comp_{comp_idx}_level_value_{level_idx}"
                                )
                            with col3:
                                if len(competency_input['levels']) > 1:
                                    if st.button(f"Delete Level {level_idx + 1}", key=f"delete_comp_{comp_idx}_level_{level_idx}", icon="🗑️", use_container_width=True):
                                        st.session_state['competency_inputs'][comp_idx]['levels'].pop(level_idx)
                                        st.rerun()
                            
                            st.session_state['competency_inputs'][comp_idx]['levels'][level_idx]['description'] = st.text_area(
                                f"Deskripsi Level {level_idx + 1}",
                                value=level_input['description'],
                                key=f"comp_{comp_idx}_level_desc_{level_idx}"
                            )
                        
                        # Add Level button for this competency
                        if st.button(f"Tambah Level", key=f"add_level_comp_{comp_idx}", use_container_width=True, icon="➕"):
                            st.session_state['competency_inputs'][comp_idx]['levels'].append({"name": "", "value": 0, "description": ""})
                            st.rerun()
            
            # Add Competency button
            if st.button("Tambah Kompetensi", use_container_width=True, icon="➕", key="add_competency_btn"):
                st.session_state['competency_inputs'].append({"competency": "", "description": "", "levels": [{"name": "", "value": 0, "description": ""}]})
                st.rerun()

        # Form for final submission
        with st.form(key='submit_form'):
            st.write("**Masukkan semua kompetensi ke database**")
            submit_name_product = st.text_input('Konfirmasi Nama Set Kompetensi', value=input_name_product, key='name_competency_set_submit')
            
            # Submit all competencies and levels to DB
            submit_button = st.form_submit_button("Simpan Set Kompetensi", use_container_width=True, icon="💾")
            
        if submit_button:
            # Check if there are any competencies with valid data
            valid_competencies = []
            for comp in st.session_state.get('competency_inputs', []):
                if comp['competency'].strip() and comp['description'].strip():
                    valid_competencies.append(comp)
            
            if valid_competencies:
                if submit_name_product.strip() == "":
                    st.error("Please enter the Name Set Kompetensi before submitting.")
                else:
                    if is_product_exists(submit_name_product):
                        st.error(f"Nama produk '{submit_name_product}' sudah ada. Mohon gunakan nama lain.")
                    else:
                        try:
                            conn = create_db_connection()
                            cursor = conn.cursor()
                            query_product = """
                                INSERT INTO pito_product (name_product)
                                VALUES (%s)
                            """
                            cursor.execute(query_product, (submit_name_product,))
                            conn.commit()
                            id_product = cursor.lastrowid

                            # Save competencies and levels
                            for comp in valid_competencies:
                                cursor.execute("""
                                    INSERT INTO pito_competency (id_product, competency, description)
                                    VALUES (%s, %s, %s)
                                """, (id_product, comp['competency'], comp['description']))
                                conn.commit()
                                id_competency = cursor.lastrowid

                                # Save levels for this competency
                                for lvl in comp.get('levels', []):
                                    if lvl['name'].strip():  # Only save levels with names
                                        cursor.execute("""
                                            INSERT INTO pito_competency_level (level_value, level_name, level_description, id_competency)
                                            VALUES (%s, %s, %s, %s)
                                        """, (lvl['value'], lvl['name'], lvl['description'], id_competency))
                                conn.commit()

                            st.success("All competencies and levels saved successfully!")
                            # Clear session state after saving
                            st.session_state['competency_inputs'] = [{"competency": "", "description": "", "levels": [{"name": "", "value": 0, "description": ""}]}]
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error saving data: {e}")
                        finally:
                            if 'cursor' in locals():
                                cursor.close()
                            if 'conn' in locals():
                                conn.close()
            else:
                st.error("No valid competencies to submit. Please add competencies with names and descriptions first.")
                

    ########################SUBTAB 2
    with subtab2:
        with st.container(border=True):
            st.subheader("Menambahkan Set Level")
            st.write("""
            Berikut adalah fitur dimana Anda bisa menambahkan set level secara mandiri.Set level yang sudah diinput disini akan muncul di tab Parameter pada  pilihan set level. Cara menambahkan level dengan:\n
            1. Masukkan nama level pada input Nama Set Level\n
            2. Masukkan nama pada input Nama Level 1 dan berikan value yang sesuai. \n
                    Contoh:
                     - level 1 maka valuenya 1, level 2 maka valuenya 2 dst.
                     - very low maka valuenya 1, low valuenya 2 dst.
            3. Klik add level jika membutuhkan nama level baru (ada kemungkinan delay jadi jika tidak muncul bisa di klik lagi)\n
            4. Anda bisa klik delete pada nama level yang tidak sesuai.\n
            5. Lakukan cek kembali lalu jika sudah yakin klik Simpan Set Level
            """)
        if 'new_levels_name' not in st.session_state:
            st.session_state['new_levels_name'] = []
        if 'new_levels_value' not in st.session_state:
            st.session_state['new_levels_value'] = []

        with st.container(border=True):
            def save_level_set_to_db(level_set_name, levels_name, levels_value):
                conn = create_db_connection()
                cursor = conn.cursor()

                try:
                    query_check_existing = """
                        SELECT COUNT(*)
                        FROM pito_level 
                        WHERE id_level_set = %s
                    """

                    cursor.execute(query_check_existing, (level_set_name,))
                    existing_count = cursor.fetchone()[0]

                    if existing_count > 0:
                        st.error(f"{level_set_name} sudah ada, mohon gunakan nama lain")
                        return

                    query_insert_level = """
                        INSERT INTO pito_level (name_level, value_level, id_level_set)
                        VALUES (%s, %s, %s)
                    """
                    for name, value in zip(levels_name, levels_value):
                        cursor.execute(query_insert_level, (name, value, level_set_name))
                    
                    conn.commit()
                
                except Exception as e:
                    st.error(f"Error saat menyimpan level set: {e}")
                
                finally:
                    cursor.close()
                    conn.close()

            def get_existing_levels(level_set_name):
                conn = create_db_connection()
                cursor = conn.cursor()

                query = """
                    SELECT name_level, value_level
                    FROM pito_level
                    WHERE id_level_set = %s
                """

                cursor.execute(query, (level_set_name,))
                result = cursor.fetchall()
                cursor.close()
                conn.close()

                return result

            level_set_name = st.text_input("Nama Set Level", key="tab5_level_set")

            if level_set_name:
                existing_levels = get_existing_levels(level_set_name)
                if existing_levels:
                    st.warning(f"Set level '{level_set_name}' sudah ada, menampilkan level yang sudah ada.")
                    if not st.session_state['new_levels_name']: 
                        for name, value in existing_levels:
                            st.session_state['new_levels_name'].append(name)
                            st.session_state['new_levels_value'].append(value)
            
            with st.container(border=True):
                if 'level_inputs' not in st.session_state:
                    st.session_state['level_inputs'] = [{"name": "", "value": 0}]

                for i, level_input in enumerate(st.session_state['level_inputs']):
                    col1, col2, col3 = st.columns([4, 2, 1], vertical_alignment="bottom")
                    with col1:
                        st.session_state['level_inputs'][i]['name'] = st.text_input(f"Nama Level {i+1}", value=level_input['name'], key=f"tab5_nama_level_{i}")
                    with col2:
                        st.session_state['level_inputs'][i]['value'] = st.number_input(f"Value Level {i+1}", value=level_input['value'], step=1, key=f"tab5_value_level_{i}")
                    with col3:
                        if st.button("Delete", key=f"remove_level_{i}"):
                            st.session_state['level_inputs'].pop(i)

                if st.button("Tambah Level", use_container_width=True, icon="➕"):
                    st.session_state['level_inputs'].append({"name": "", "value": 0})

            if st.session_state['new_levels_name']:
                st.write("Level yang sudah ditambahkan:")
                for i, (name, value) in enumerate(zip(st.session_state['new_levels_name'], st.session_state['new_levels_value'])):
                    st.write(f"{i+1}. Nama Level: {name}, Value Level: {value}")

                    if st.button(f"Hapus Level {name}", key=f"delete_{i}"):
                        st.session_state['new_levels_name'].pop(i)
                        st.session_state['new_levels_value'].pop(i)
                        st.success(f"Level '{name}' berhasil dihapus.")
                        st.experimental_rerun() 
            
        if st.button("Simpan Set Level", use_container_width=True, icon="💾", key="save_level"):
            if level_set_name and 'level_inputs' in st.session_state:
                filtered_levels = [lvl for lvl in st.session_state['level_inputs'] if lvl['name'].strip() != '']

                if not filtered_levels:
                    st.error("Mohon masukkan setidaknya satu nama level yang valid sebelum menyimpan.")
                else:
                    filtered_names = [lvl['name'] for lvl in filtered_levels]
                    filtered_values = [lvl['value'] for lvl in filtered_levels]
                    save_level_set_to_db(level_set_name, filtered_names, filtered_values)
                    
                    st.session_state['level_inputs'] = [{"name": "", "value": 0}]
                    st.success("Set level berhasil ditambahkan!")
            else:
                st.error("Mohon masukkan nama set level dan setidaknya satu level sebelum menyimpan.")

    ########################TAB 6
    with subtab3:
        with st.container(border=True):
            st.subheader("Menambahkan Assessor Baru")
            st.write("""
            Berikut adalah fitur dimana Anda bisa menambahkan kode assessor baru secara mandiri. Cara menambahkan kode assessor dengan:\n
            1. Masukkan Kode Assessor dengan huruf kapital.\n
            2. Masukkan Nama Assessor.\n
            3. Periksa kembali inputan Anda dan klik Simpan jika sudah benar.
            """)
        def get_existing_assessor(assessor_code):
            conn = create_db_connection()
            cursor = conn.cursor()

            query = """
                SELECT kode_assessor, name_assessor
                FROM txtan_assessor
                WHERE kode_assessor = %s
            """

            cursor.execute(query, (assessor_code,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()

            return result
        
        def save_assessor_to_db(assessor_code, name_assessor):
            conn = create_db_connection()
            cursor = conn.cursor()

            try:
                existing_assessor = get_existing_assessor(assessor_code)

                if existing_assessor:
                    existing_name_assessor = existing_assessor[1]
                    st.error(f'Assessor dengan kode {assessor_code} sudah digunakan oleh {existing_name_assessor}, mohon gunakan kode lain.')
                    return

                query_insert_assessor = """
                INSERT INTO txtan_assessor (kode_assessor, name_assessor)
                VALUES (%s, %s)
                """
                cursor.execute(query_insert_assessor, (assessor_code, name_assessor))
                conn.commit()
                st.success(f"Assessor {name_assessor} dengan kode {assessor_code} berhasil disimpan")

            except Exception as e:
                st.error(f"Error saat menyimpan kode assessor: {e}")

            finally:
                cursor.close()
                conn.close()
        with st.container(border=True):
            
            input_assessor_code = st.text_input("Kode Assessor (Huruf Kapital)")
            input_assessor_name = st.text_input("Nama Assessor")

        if st.button("Simpan Assessor", use_container_width=True, icon="💾"):
            if input_assessor_code and input_assessor_name:
                save_assessor_to_db(input_assessor_code, input_assessor_name)
            else:
                st.error("Mohon masukkan kode dan nama assessor.")
