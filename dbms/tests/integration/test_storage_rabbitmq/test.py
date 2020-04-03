import os.path as p
import random
import threading
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess

import kafka # for rabbitmq

from google.protobuf.internal.encoder import _VarintBytes


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/rabbitmq.xml','configs/log_conf.xml'],
                                with_rabbitmq=True,
                                clickhouse_path_dir='clickhouse_path')
rabbitmq_id = ''


# Helpers


# def wait_rabbitmq_is_available(max_retries=50):
#     retries = 0
#     while True:
#         if check_rabbitmq_is_available():
#             break
#         else:
#             retries += 1
#             if retries > max_retries:
#                 raise "RabbitMQ is not available"
#             print("Waiting for RabbitMQ to start up")
#             time.sleep(1)


def rabbitmq_produce(channel, key, messages):
    for message in messages:
        channel.basic_publish(exchange='', routing_key=key, body=message)


def rabbitmq_consume(channel, queue_name):
    channel.basic_consume(queue=queue_name, auto_ack=True)


def rabbitmq_check_result(result, check=False, ref_file='test_rabbitmq_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        global rabbitmq_id
        cluster.start()
        rabbitmq_id = instance.cluster.rabbitmq_docker_id
        print("rabbitmq_id is {}".format(rabbitmq_id))
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')
    # wait_rabbitmq_is_available()
    print("RabbitMQ is available - running test")
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests


@pytest.mark.timeout(180)
def test_rabbitmq_settings_new_syntax(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'localhost:5672',
                     rabbitmq_routing_key_list = 'new',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    rabbitmq_produce(channel, 'new', messages)

    # Insert couple of malformed messages.
    rabbitmq_produce(channel, 'new', ['}{very_broken_message,'])
    rabbitmq_produce(channel, 'new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    rabbitmq_produce('new', messages)

    connection.close();

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
