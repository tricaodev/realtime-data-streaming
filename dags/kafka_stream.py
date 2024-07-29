import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import time
import logging

def get_data():
    res = requests.get('https://randomuser.me/api/').json()
    return res['results'][0]

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    location = res['location']
    data['address'] = '{} {}, {}, {}, {}'.format(location['street']['number'], location['street']['name'], location['state'],
                                                  location['city'], location['country'])
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    producer = KafkaProducer(
        bootstrap_servers='broker:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000
    )
    current_time = time.time()
    while True:
        if time.time() > current_time + 60:
            break
        try:
            data = format_data(get_data())
            producer.send('users_created', data)
        except Exception as e:
            logging.error("An error occurred: {}".format(e))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 1),
}

with DAG('user_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )

