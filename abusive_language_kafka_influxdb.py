# Program to copy data from kafka producer and move to influxdb database.
# Following is the data being processed
'''
{
    "specversion": "1.0",
    "type": "reviews",
    "source": "reviews-generators",
    "id": "e5443902-ff87-414b-8ec8-92da00e641c5",
    "time": "2023-06-15T19:36:24.895029Z",
    "subject": "product-reviews",
    "data": {
        "product": {
            "product_id": "829300",
            "product_name": "Quarkus T-shirt",
            "category": "clothing"
        },
        "user": {
            "name": "Alison Silva",
            "customer_id": "asilva",
            "browser": "Chrome",
            "region": "India"
        },
        "rating": 0,
        "timestamp": "2023-06-15T19:36:24.895029Z",
        "review_text": "This is good way to go to market",
        "score": -1,
        "response": "Non-Abusive"
    }
}
'''

from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from transformers import pipeline
import ssl
import json
import os
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from datetime import datetime
from ssl import SSLContext, PROTOCOL_TLSv1
from influxdb import InfluxDBClient
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions
from dateutil import parser

''' 
Contents of requirements.txt
kafka-python
transformers
torch
pytorch
influxdb
influxdb-client
'''

bucket = "sentiment_analysis_data"
org = "globex"
token = "mwT6TRX-oeSgg0V78bsaDm5FykP-PWY3Yj-Jw2wRPHEXu9z48JU9B2HqUBmaXVqbn4hlzbkCO1Fu0LyXnHsUew=="
# Store the URL of your InfluxDB instance
url="http://localhost:8086"

client = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)

#bootstrap_servers = ['globex-ret-cgbv--fs---qsv-ajjig.bf2.kafka.rhcloud.com:443']
topic = 'consume-topic'
produce_topic = 'produce-topic'
bootstrap_servers = ['localhost:9092']
#username = '5c8037b6-8d56-4160-8ea5-e6921d1da5fb'
#password = 'affyRJrfk2eYJBcQtQDstnVUGcLXA78U'
#sasl_mechanism = 'PLAIN'
#security_protocol = 'SASL_SSL'

consumer = KafkaConsumer(
    produce_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
#    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Set up a Kafka consumer
#consumer = KafkaConsumer(
#    topic,
#    bootstrap_servers=bootstrap_servers,
#    sasl_plain_username=username,
#    sasl_plain_password=password,
#    security_protocol=security_protocol,
#    sasl_mechanism=sasl_mechanism,
#    auto_offset_reset='latest',
#    enable_auto_commit=True,
#    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
#    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

#influxdb_host = 'localhost'
#influxdb_port = 8086
#influxdb_dbname = 'globex'
influxdb_measurement = 'globex-measurement'
#influxdb_username = 'globex'
#influxdb_password = 'globex-password'
#influxdb_client = InfluxDBClient(influxdb_host, influxdb_port, influxdb_username, influxdb_password)
#influxdb_client.switch_database(influxdb_dbname)

# Set up a Kafka producer
#producer = KafkaProducer(
#    bootstrap_servers=bootstrap_servers,
#    sasl_plain_username=username,
#    sasl_plain_password=password,
#    security_protocol=security_protocol,
#    sasl_mechanism=sasl_mechanism,
#    value_serializer=lambda m: json.dumps(m).encode('utf-8')
#)

# Start consuming Kafka messages
for message in consumer:
    try:
        # Get the text message from the Kafka message
        #print(message)
        json_payload = message.value
        # Parse the CloudEvent from the JSON payload
        json_data = json.loads(json_payload)
        '''
        json_body = [
            {
                "measurement": influxdb_measurement,
                "time": json_data['time'],
                "fields": {
                    "rating": json_data['data']['rating'],
                    "timestamp": json_data['data']['timestamp'],
                    "score": json_data['data']['score']
                },
                "tags": {
                    "product_id": json_data['data']['product']['product_id'],
                    "product_name": json_data['data']['product']['product_name'],
                    "category": json_data['data']['product']['category'],
                    "name": json_data['data']['user']['name'],
                    "customer_id": json_data['data']['user']['customer_id'],
                    "browser": json_data['data']['user']['browser'],
                    "region": json_data['data']['user']['region'],
                    "response": json_data['data']['response']
                }
            }
        ]'''
        json_data_data = json_data["data"]

        # Create a new InfluxDB data point
        point = influxdb_client.Point(bucket)

        # Set the time for the data point
        point.time(parser.parse(json_data["time"]))

        # Flatten the "product" field
        product = json_data_data["product"]

        # Set the remaining fields and tags
        for key, value in json_data_data["product"].items():
            if key != "product":
                if isinstance(value, dict):
                    # Handle other nested fields if needed
                    pass
                else:
                    point.field(key, value)

            with InfluxDBClient(url, token) as client:
                with client.write_api(write_options=SYNCHRONOUS) as writer:
                    try:
                        writer.write(bucket, org="globex", record=[point])
                    except InfluxDBError as e:
                        print(e)

        # Flatten the "user" field
        user = json_data_data["user"]

        # Set the remaining fields and tags
        for key, value in json_data_data["user"].items():
            if key != "user":
                if isinstance(value, dict):
                    # Handle other nested fields if needed
                    pass
                else:
                    point.field(key, value)

        with InfluxDBClient(url, token) as client:
            with client.write_api(write_options=SYNCHRONOUS) as writer:
                try:
                    writer.write(bucket, org="globex", record=[point])
                except InfluxDBError as e:
                    print(e)

        # Set the remaining fields and tags
        for key, value in json_data_data.items():
            if key != "user" or key != "data":
                if isinstance(value, dict):
                    # Handle other nested fields if needed
                    pass
                else:
                    point.field(key, value)

        with InfluxDBClient(url, token) as client:
            with client.write_api(write_options=SYNCHRONOUS) as writer:
                try:
                    writer.write(bucket, org="globex", record=[point])
                except InfluxDBError as e:
                    print(e)

        # Flatten the "attributes" field
        for key, value in json_data.items():
            if key != "data":
                if isinstance(value, dict):
                    # Handle other nested fields if needed
                    pass
                else:
                    point.field(key, value)

        with InfluxDBClient(url, token) as client:
            with client.write_api(write_options=SYNCHRONOUS) as writer:
                try:
                    writer.write(bucket, org="globex", record=[point])
                except InfluxDBError:
                    print("Data incorrect, skipping")

    except json.JSONDecodeError:
        print("Non-JSON message received, skipping...")
    except KeyError:
        print("Missing fields in JSON message, skipping...")
