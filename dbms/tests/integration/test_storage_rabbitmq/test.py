import os.path as p
import random
import threading
import time
import pytest

import pika

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

def check_rabbitmq_is_available():
    # works but too many logs when connection fails
    #try:
    #    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
    #    if connection.is_open:
    #        connection.close()
    #        return 1
    #except:
    #    return 0

    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          'rabbitmqctl', 
                          'await_startup'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def wait_rabbitmq_is_available(max_retries=50):
    retries = 0
    while True:
        if check_rabbitmq_is_available():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "RabbitMQ is not available"
            print("Waiting for RabbitMQ to start up")
            time.sleep(1)


def rabbitmq_produce(channel, key, messages):
    channel.exchange_declare(exchange='new_exch', exchange_type='direct')
    for message in messages:
        channel.basic_publish(exchange='new_exch', routing_key=key, body=message)


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
    wait_rabbitmq_is_available()
    print("RabbitMQ is available - running test")
    yield  # run test
    #instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests

def callback(ch, method, properties, body):
    print("%r:%r" % (method.routing_key, body))
    assert 0


@pytest.mark.timeout(180)
def test_rabbitmq_settings_new_syntax(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:15672',
                     rabbitmq_routing_key_list = 'new',
                     rabbitmq_exchange_name = 'direct_exchange',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))

    #channel_cons = connection.channel()
    #channel_cons.exchange_declare(exchange='direct_exchange', exchange_type='direct')
    #result = channel_cons.queue_declare(queue='', exclusive=True)
    #queue_name = result.method.queue
    #channel_cons.queue_bind(exchange='direct_exchange', queue=queue_name, routing_key='new')
    #channel_cons.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))

    channel = connection.channel()

    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='new', body=message)

    # Insert couple of malformed messages.
#    rabbitmq_produce(channel, 'new', ['}{very_broken_message,'])
#    rabbitmq_produce(channel, 'new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='new', body=message)

    #channel_cons.start_consuming()

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=False)
        if result == '':
            break
        if rabbitmq_check_result(result):
            break

    connection.close()

    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_select_empty(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:15672',
                     rabbitmq_routing_key_list = 'empty',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    assert int(instance.query('SELECT count() FROM test.rabbitmq')) == 0


#@pytest.mark.timeout(180)
#def test_rabbitmq_materialized_view(rabbitmq_cluster):
#    instance.query('''
#        DROP TABLE IF EXISTS test.view;
#        DROP TABLE IF EXISTS test.consumer;
#        CREATE TABLE IF NOT EXISTS test.rabbitmq (key UInt64, value UInt64)
#            ENGINE = RabbitMQ
#            SETTINGS rabbitmq_host_port = 'rabbitmq1:15672',
#                     rabbitmq_routing_key_list = 'mv',
#                     rabbitmq_format = 'JSONEachRow',
#                     rabbitmq_row_delimiter = '\\n';
#        CREATE TABLE test.view (key UInt64, value UInt64)
#            ENGINE = MergeTree()
#            ORDER BY key;
#        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#            SELECT * FROM test.rabbitmq;
#    ''')
#
#    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
#    channel = connection.channel()
#
#    messages = []
#    for i in range(50):
#        messages.append(json.dumps({'key': i, 'value': i}))
#    rabbitmq_produce(channel, 'mv', messages)
#
#    while True:
#        result = instance.query('SELECT * FROM test.view')
#        if rabbitmq_check_result(result):
#            break
#
#    connection.close()
#
#    instance.query('''
#        DROP TABLE test.consumer;
#        DROP TABLE test.view;
#    ''')
#
#    rabbitmq_check_result(result, True)


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
