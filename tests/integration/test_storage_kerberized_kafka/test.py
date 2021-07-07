import os.path as p
import random
import threading
import time
import pytest
import logging

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess
import kafka.errors
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsResponse_v1, DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment
import socket

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                main_configs=['configs/kafka.xml'],
                                with_kerberized_kafka=True,
                                clickhouse_path_dir="clickhouse_path")

def producer_serializer(x):
    return x.encode() if isinstance(x, str) else x

def get_kafka_producer(port, serializer):
    errors = []
    for _ in range(15):
        try:
            producer = KafkaProducer(bootstrap_servers="localhost:{}".format(port), value_serializer=serializer)
            logging.debug("Kafka Connection establised: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)
    
    raise Exception("Connection not establised, {}".format(errors))   

def kafka_produce(kafka_cluster, topic, messages, timestamp=None):
    logging.debug("kafka_produce server:{}:{} topic:{}".format("localhost", kafka_cluster.kerberized_kafka_port, topic))
    producer = get_kafka_producer(kafka_cluster.kerberized_kafka_port, producer_serializer)
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()


# Fixtures

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP DATABASE IF EXISTS test; CREATE DATABASE test;')
    yield  # run test

# Tests

def test_kafka_json_as_string(kafka_cluster):
    kafka_produce(kafka_cluster, 'kafka_json_as_string', ['{"t": 123, "e": {"x": "woof"} }', '', '{"t": 124, "e": {"x": "test"} }', '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}'])

    instance.query('''
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string',
                     kafka_group_name = 'kafka_json_as_string',
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        ''')

    time.sleep(3)

    result = instance.query('SELECT * FROM test.kafka;')
    expected = '''\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
'''
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log("Parsing of message (topic: kafka_json_as_string, partition: 0, offset: 1) return no rows")

def test_kafka_json_as_string_no_kdc(kafka_cluster):
    kafka_produce(kafka_cluster, 'kafka_json_as_string_no_kdc', ['{"t": 123, "e": {"x": "woof"} }', '', '{"t": 124, "e": {"x": "test"} }', '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}'])

    kafka_cluster.pause_container('kafka_kerberos')
    time.sleep(45)   # wait for ticket expiration

    instance.query('''
        CREATE TABLE test.kafka_no_kdc (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string_no_kdc',
                     kafka_group_name = 'kafka_json_as_string_no_kdc',
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        ''')

    result = instance.query('SELECT * FROM test.kafka_no_kdc;')
    expected = ''

    kafka_cluster.unpause_container('kafka_kerberos')


    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log("StorageKafka (kafka_no_kdc): Nothing to commit")
    assert instance.contains_in_log("Ticket expired")
    assert instance.contains_in_log("Kerberos ticket refresh failed")


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
