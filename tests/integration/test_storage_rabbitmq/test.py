import json
import os.path as p
import random
import subprocess
import threading
import logging
import time
from random import randrange

import pika
import pytest
from google.protobuf.internal.encoder import _VarintBytes
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

from . import rabbitmq_pb2

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                main_configs=['configs/rabbitmq.xml'],
                                with_rabbitmq=True)


# Helpers


def rabbitmq_check_result(result, check=False, ref_file='test_rabbitmq_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


def kill_rabbitmq(rabbitmq_id):
    p = subprocess.Popen(('docker', 'stop', rabbitmq_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def revive_rabbitmq(rabbitmq_id):
    p = subprocess.Popen(('docker', 'start', rabbitmq_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        logging.debug("rabbitmq_id is {}".format(instance.cluster.rabbitmq_docker_id))
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    print("RabbitMQ is available - running test")
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests

def test_rabbitmq_select(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_exchange_name = 'select',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        '''.format(rabbitmq_cluster.rabbitmq_host))

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))

    for message in messages:
        channel.basic_publish(exchange='select', routing_key='', body=message)

    connection.close()
    # The order of messages in select * from test.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq ORDER BY key', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


def test_rabbitmq_select_empty(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_exchange_name = 'empty',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        '''.format(rabbitmq_cluster.rabbitmq_host))

    assert int(instance.query('SELECT count() FROM test.rabbitmq')) == 0


def test_rabbitmq_json_without_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_exchange_name = 'json',
                     rabbitmq_format = 'JSONEachRow'
        '''.format(rabbitmq_cluster.rabbitmq_host))

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'

    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='json', routing_key='', body=message)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='json', routing_key='', body=message)

    connection.close()
    time.sleep(1)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq ORDER BY key', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


def test_rabbitmq_csv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'csv',
                     rabbitmq_format = 'CSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='csv', routing_key='', body=message)

    connection.close()
    time.sleep(1)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq ORDER BY key', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


def test_rabbitmq_tsv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'tsv',
                     rabbitmq_format = 'TSV',
                     rabbitmq_queue_base = 'tsv',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='tsv', routing_key='', body=message)
    connection.close()

    result = ''
    while True:
        result = instance.query('SELECT * FROM test.view ORDER BY key')
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


def test_rabbitmq_materialized_view(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='mv', routing_key='', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view ORDER BY key')
        if (rabbitmq_check_result(result)):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close()
    rabbitmq_check_result(result, True)


def test_rabbitmq_materialized_view_with_subquery(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mvsq',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.rabbitmq);
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='mvsq', routing_key='', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view ORDER BY key')
        if rabbitmq_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close()
    rabbitmq_check_result(result, True)


def test_rabbitmq_many_materialized_views(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mmv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.rabbitmq;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='mmv', routing_key='', body=message)

    while True:
        result1 = instance.query('SELECT * FROM test.view1 ORDER BY key')
        result2 = instance.query('SELECT * FROM test.view2 ORDER BY key')
        if rabbitmq_check_result(result1) and rabbitmq_check_result(result2):
            break

    instance.query('''
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    ''')

    connection.close()
    rabbitmq_check_result(result1, True)
    rabbitmq_check_result(result2, True)


@pytest.mark.skip(reason="clichouse_path with rabbitmq.proto fails to be exported")
def test_rabbitmq_protobuf(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'pb',
                     rabbitmq_format = 'Protobuf',
                     rabbitmq_schema = 'rabbitmq.proto:KeyValueProto';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    data = ''
    for i in range(0, 20):
        msg = rabbitmq_pb2.KeyValueProto()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    channel.basic_publish(exchange='pb', routing_key='', body=data)
    data = ''
    for i in range(20, 21):
        msg = rabbitmq_pb2.KeyValueProto()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    channel.basic_publish(exchange='pb', routing_key='', body=data)
    data = ''
    for i in range(21, 50):
        msg = rabbitmq_pb2.KeyValueProto()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    channel.basic_publish(exchange='pb', routing_key='', body=data)

    connection.close()

    result = ''
    while True:
        result = instance.query('SELECT * FROM test.view ORDER BY key')
        if rabbitmq_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    rabbitmq_check_result(result, True)


def test_rabbitmq_big_message(rabbitmq_cluster):
    # Create batchs of messages of size ~100Kb
    rabbitmq_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(rabbitmq_messages)]

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'big',
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    for message in messages:
        channel.basic_publish(exchange='big', routing_key='', body=message)

    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == batch_messages * rabbitmq_messages:
            break

    connection.close()
    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == rabbitmq_messages * batch_messages, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_sharding_between_queues_publish(rabbitmq_cluster):
    NUM_CONSUMERS = 10
    NUM_QUEUES = 10

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'test_sharding',
                     rabbitmq_num_queues = 10,
                     rabbitmq_num_consumers = 10,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64, channel_id String)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _channel_id AS channel_id FROM test.rabbitmq;
    ''')

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        current = 0
        for message in messages:
            current += 1
            mes_id = str(current)
            channel.basic_publish(exchange='test_sharding', routing_key='',
                                  properties=pika.BasicProperties(message_id=mes_id), body=message)
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ''
    while True:
        result1 = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.view")

    for thread in threads:
        thread.join()

    assert int(result1) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)
    assert int(result2) == 10


def test_rabbitmq_mv_combo(rabbitmq_cluster):
    NUM_MV = 5
    NUM_CONSUMERS = 4

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'combo',
                     rabbitmq_queue_base = 'combo',
                     rabbitmq_num_consumers = 2,
                     rabbitmq_num_queues = 5,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    for mv_id in range(NUM_MV):
        instance.query('''
            DROP TABLE IF EXISTS test.combo_{0};
            DROP TABLE IF EXISTS test.combo_{0}_mv;
            CREATE TABLE test.combo_{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.combo_{0}_mv TO test.combo_{0} AS
                SELECT * FROM test.rabbitmq;
        '''.format(mv_id))

    time.sleep(2)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(exchange='combo', routing_key='',
                                  properties=pika.BasicProperties(message_id=str(msg_id)), body=messages[msg_id])
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = 0
        for mv_id in range(NUM_MV):
            result += int(instance.query('SELECT count() FROM test.combo_{0}'.format(mv_id)))
        if int(result) == messages_num * threads_num * NUM_MV:
            break
        time.sleep(1)

    for thread in threads:
        thread.join()

    for mv_id in range(NUM_MV):
        instance.query('''
            DROP TABLE test.combo_{0}_mv;
            DROP TABLE test.combo_{0};
        '''.format(mv_id))

    assert int(result) == messages_num * threads_num * NUM_MV, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_insert(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert1',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue='')
    queue_name = result.method.queue
    consumer.queue_bind(exchange='insert', queue=queue_name, routing_key='insert1')

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    insert_messages = []

    def onReceived(channel, method, properties, body):
        i = 0
        insert_messages.append(body.decode())
        if (len(insert_messages) == 50):
            channel.stop_consuming()

    consumer.basic_consume(onReceived, queue_name)
    consumer.start_consuming()
    consumer_connection.close()

    result = '\n'.join(insert_messages)
    rabbitmq_check_result(result, True)


def test_rabbitmq_insert_headers_exchange(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert_headers',
                     rabbitmq_exchange_type = 'headers',
                     rabbitmq_routing_key_list = 'test=insert,topic=headers',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue='')
    queue_name = result.method.queue
    consumer.queue_bind(exchange='insert_headers', queue=queue_name, routing_key="",
                        arguments={'x-match': 'all', 'test': 'insert', 'topic': 'headers'})

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    insert_messages = []

    def onReceived(channel, method, properties, body):
        i = 0
        insert_messages.append(body.decode())
        if (len(insert_messages) == 50):
            channel.stop_consuming()

    consumer.basic_consume(onReceived, queue_name)
    consumer.start_consuming()
    consumer_connection.close()

    result = '\n'.join(insert_messages)
    rabbitmq_check_result(result, True)


def test_rabbitmq_many_inserts(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.rabbitmq_many;
        DROP TABLE IF EXISTS test.rabbitmq_consume;
        DROP TABLE IF EXISTS test.view_many;
        DROP TABLE IF EXISTS test.consumer_many;
        CREATE TABLE test.rabbitmq_many (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'many_inserts',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'many_inserts',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    messages_num = 10000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    def insert():
        while True:
            try:
                instance.query("INSERT INTO test.rabbitmq_many VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query('''
        CREATE TABLE test.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.rabbitmq_consume;
    ''')

    for thread in threads:
        thread.join()

    while True:
        result = instance.query('SELECT count() FROM test.view_many')
        print(result, messages_num * threads_num)
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)

    instance.query('''
        DROP TABLE test.rabbitmq_consume;
        DROP TABLE test.rabbitmq_many;
        DROP TABLE test.consumer_many;
        DROP TABLE test.view_many;
    ''')

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_overloaded_insert(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        DROP TABLE IF EXISTS test.rabbitmq_consume;
        CREATE TABLE test.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'over',
                     rabbitmq_queue_base = 'over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_num_consumers = 5,
                     rabbitmq_num_queues = 10,
                     rabbitmq_max_block_size = 10000,
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.rabbitmq_overload (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.rabbitmq_consume;
    ''')

    messages_num = 100000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

        while True:
            try:
                instance.query("INSERT INTO test.rabbitmq_overload VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 5
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view_overload')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE test.consumer_overload;
        DROP TABLE test.view_overload;
        DROP TABLE test.rabbitmq_consume;
        DROP TABLE test.rabbitmq_overload;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_direct_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print(("Setting up table {}".format(consumer_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.direct_exchange_{0};
            DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
            CREATE TABLE test.direct_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_name = 'direct_exchange_testing',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_routing_key_list = 'direct_{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.direct_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.direct_exchange_{0};
        '''.format(consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    key_num = 0
    for num in range(num_tables):
        key = "direct_" + str(key_num)
        key_num += 1
        for message in messages:
            mes_id = str(randrange(10))
            channel.basic_publish(
                exchange='direct_exchange_testing', routing_key=key,
                properties=pika.BasicProperties(message_id=mes_id), body=message)

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables:
            break

    for consumer_id in range(num_tables):
        instance.query('''
            DROP TABLE test.direct_exchange_{0}_mv;
            DROP TABLE test.direct_exchange_{0};
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_fanout_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print(("Setting up table {}".format(consumer_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.fanout_exchange_{0};
            DROP TABLE IF EXISTS test.fanout_exchange_{0}_mv;
            CREATE TABLE test.fanout_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_routing_key_list = 'key_{0}',
                         rabbitmq_exchange_name = 'fanout_exchange_testing',
                         rabbitmq_exchange_type = 'fanout',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.fanout_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.fanout_exchange_{0};
        '''.format(consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    for msg_id in range(messages_num):
        channel.basic_publish(exchange='fanout_exchange_testing', routing_key='',
                              properties=pika.BasicProperties(message_id=str(msg_id)), body=messages[msg_id])

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables:
            break

    for consumer_id in range(num_tables):
        instance.query('''
            DROP TABLE test.fanout_exchange_{0}_mv;
            DROP TABLE test.fanout_exchange_{0};
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE test.destination;
    ''')

    assert int(result) == messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_topic_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print(("Setting up table {}".format(consumer_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.topic_exchange_{0};
        '''.format(consumer_id))

    for consumer_id in range(num_tables):
        print(("Setting up table {}".format(num_tables + consumer_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.logs',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.topic_exchange_{0};
        '''.format(num_tables + consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    key_num = 0
    for num in range(num_tables):
        key = "topic." + str(key_num)
        key_num += 1
        for message in messages:
            channel.basic_publish(exchange='topic_exchange_testing', routing_key=key, body=message)

    key = "random.logs"
    current = 0
    for msg_id in range(messages_num):
        channel.basic_publish(exchange='topic_exchange_testing', routing_key=key,
                              properties=pika.BasicProperties(message_id=str(msg_id)), body=messages[msg_id])

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables + messages_num * num_tables:
            break

    for consumer_id in range(num_tables * 2):
        instance.query('''
            DROP TABLE test.topic_exchange_{0}_mv;
            DROP TABLE test.topic_exchange_{0};
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE test.destination;
    ''')

    assert int(
        result) == messages_num * num_tables + messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(
        result)


def test_rabbitmq_hash_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 4
    for consumer_id in range(num_tables):
        table_name = 'rabbitmq_consumer{}'.format(consumer_id)
        print(("Setting up {}".format(table_name)))
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 4,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_type = 'consistent_hash',
                         rabbitmq_exchange_name = 'hash_exchange_testing',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
                SELECT key, value, _channel_id AS channel_id FROM test.{0};
        '''.format(table_name))

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(exchange='hash_exchange_testing', routing_key=str(msg_id),
                                  properties=pika.BasicProperties(message_id=str(msg_id)), body=messages[msg_id])
        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ''
    while True:
        result1 = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")

    for consumer_id in range(num_tables):
        table_name = 'rabbitmq_consumer{}'.format(consumer_id)
        instance.query('''
            DROP TABLE test.{0}_mv;
            DROP TABLE test.{0};
        '''.format(table_name))

    instance.query('''
        DROP TABLE test.destination;
    ''')

    for thread in threads:
        thread.join()

    assert int(result1) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)
    assert int(result2) == 4 * num_tables


def test_rabbitmq_multiple_bindings(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    instance.query('''
        DROP TABLE IF EXISTS test.bindings;
        DROP TABLE IF EXISTS test.bindings_mv;
        CREATE TABLE test.bindings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'multiple_bindings_testing',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'key1,key2,key3,key4,key5',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.bindings_mv TO test.destination AS
            SELECT * FROM test.bindings;
    ''')

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1

        keys = ['key1', 'key2', 'key3', 'key4', 'key5']

        for key in keys:
            for message in messages:
                channel.basic_publish(exchange='multiple_bindings_testing', routing_key=key, body=message)

        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * threads_num * 5:
            break

    for thread in threads:
        thread.join()

    instance.query('''
        DROP TABLE test.bindings;
        DROP TABLE test.bindings_mv;
        DROP TABLE test.destination;
    ''')

    assert int(result) == messages_num * threads_num * 5, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_headers_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables_to_receive = 2
    for consumer_id in range(num_tables_to_receive):
        print(("Setting up table {}".format(consumer_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.headers_exchange_{0};
            DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
            CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_exchange_name = 'headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2020',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.headers_exchange_{0};
        '''.format(consumer_id))

    num_tables_to_ignore = 2
    for consumer_id in range(num_tables_to_ignore):
        print(("Setting up table {}".format(consumer_id + num_tables_to_receive)))
        instance.query('''
            DROP TABLE IF EXISTS test.headers_exchange_{0};
            DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
            CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = 'headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2019',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.headers_exchange_{0};
        '''.format(consumer_id + num_tables_to_receive))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    fields = {}
    fields['format'] = 'logs'
    fields['type'] = 'report'
    fields['year'] = '2020'

    for msg_id in range(messages_num):
        channel.basic_publish(exchange='headers_exchange_testing', routing_key='',
                              properties=pika.BasicProperties(headers=fields, message_id=str(msg_id)),
                              body=messages[msg_id])

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables_to_receive:
            break

    for consumer_id in range(num_tables_to_receive + num_tables_to_ignore):
        instance.query('''
            DROP TABLE test.headers_exchange_{0}_mv;
            DROP TABLE test.headers_exchange_{0};
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE test.destination;
    ''')

    assert int(result) == messages_num * num_tables_to_receive, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_virtual_columns(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        CREATE TABLE test.rabbitmq_virtuals (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'virtuals',
                     rabbitmq_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _exchange_name, _channel_id, _delivery_tag, _redelivered FROM test.rabbitmq_virtuals;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({'key': i, 'value': i}))
        i += 1

    for message in messages:
        channel.basic_publish(exchange='virtuals', routing_key='', body=message)

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == message_num:
            break

    connection.close()

    result = instance.query('''
        SELECT key, value, _exchange_name, SUBSTRING(_channel_id, 1, 3), _delivery_tag, _redelivered
        FROM test.view ORDER BY key
    ''')

    expected = '''\
0	0	virtuals	1_0	1	0
1	1	virtuals	1_0	2	0
2	2	virtuals	1_0	3	0
3	3	virtuals	1_0	4	0
4	4	virtuals	1_0	5	0
5	5	virtuals	1_0	6	0
6	6	virtuals	1_0	7	0
7	7	virtuals	1_0	8	0
8	8	virtuals	1_0	9	0
9	9	virtuals	1_0	10	0
'''

    instance.query('''
        DROP TABLE test.rabbitmq_virtuals;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV(expected)


def test_rabbitmq_virtual_columns_with_materialized_view(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq_virtuals_mv (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'virtuals_mv',
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64,
            exchange_name String, channel_id String, delivery_tag UInt64, redelivered UInt8) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _exchange_name as exchange_name, _channel_id as channel_id, _delivery_tag as delivery_tag, _redelivered as redelivered
        FROM test.rabbitmq_virtuals_mv;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({'key': i, 'value': i}))
        i += 1

    for message in messages:
        channel.basic_publish(exchange='virtuals_mv', routing_key='', body=message)

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == message_num:
            break

    connection.close()

    result = instance.query(
        "SELECT key, value, exchange_name, SUBSTRING(channel_id, 1, 3), delivery_tag, redelivered FROM test.view ORDER BY delivery_tag")
    expected = '''\
0	0	virtuals_mv	1_0	1	0
1	1	virtuals_mv	1_0	2	0
2	2	virtuals_mv	1_0	3	0
3	3	virtuals_mv	1_0	4	0
4	4	virtuals_mv	1_0	5	0
5	5	virtuals_mv	1_0	6	0
6	6	virtuals_mv	1_0	7	0
7	7	virtuals_mv	1_0	8	0
8	8	virtuals_mv	1_0	9	0
9	9	virtuals_mv	1_0	10	0
'''

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.rabbitmq_virtuals_mv
    ''')

    assert TSV(result) == TSV(expected)


def test_rabbitmq_many_consumers_to_each_queue(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 4
    for table_id in range(num_tables):
        print(("Setting up table {}".format(table_id)))
        instance.query('''
            DROP TABLE IF EXISTS test.many_consumers_{0};
            DROP TABLE IF EXISTS test.many_consumers_{0}_mv;
            CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = 'many_consumers',
                         rabbitmq_num_queues = 2,
                         rabbitmq_num_consumers = 2,
                         rabbitmq_queue_base = 'many_consumers',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
            SELECT key, value, _channel_id as channel_id FROM test.many_consumers_{0};
        '''.format(table_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(exchange='many_consumers', routing_key='',
                                  properties=pika.BasicProperties(message_id=str(msg_id)), body=messages[msg_id])
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ''
    while True:
        result1 = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")

    for thread in threads:
        thread.join()

    for consumer_id in range(num_tables):
        instance.query('''
            DROP TABLE test.many_consumers_{0};
            DROP TABLE test.many_consumers_{0}_mv;
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE test.destination;
    ''')

    assert int(result1) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)
    # 4 tables, 2 consumers for each table => 8 consumer tags
    assert int(result2) == 8


def test_rabbitmq_restore_failed_connection_without_losses_1(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.consume;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'producer_reconnect',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_num_consumers = 2,
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        DROP TABLE IF EXISTS test.producer_reconnect;
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'producer_reconnect',
                     rabbitmq_persistent = '1',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages_num = 100000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.producer_reconnect VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        time.sleep(0.1)

    kill_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)
    time.sleep(4)
    revive_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)

    while True:
        result = instance.query('SELECT count(DISTINCT key) FROM test.view')
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.consume;
        DROP TABLE test.producer_reconnect;
    ''')

    assert int(result) == messages_num, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_restore_failed_connection_without_losses_2(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'consumer_reconnect',
                     rabbitmq_num_consumers = 10,
                     rabbitmq_num_queues = 10,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    i = 0
    messages_num = 150000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i, 'value': i}))
        i += 1
    for msg_id in range(messages_num):
        channel.basic_publish(exchange='consumer_reconnect', routing_key='', body=messages[msg_id],
                              properties=pika.BasicProperties(delivery_mode=2, message_id=str(msg_id)))
    connection.close()
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consumer_reconnect;
    ''')

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        print(3)
        time.sleep(0.1)

    kill_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)
    time.sleep(8)
    revive_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)

    # while int(instance.query('SELECT count() FROM test.view')) == 0:
    #    time.sleep(0.1)

    # kill_rabbitmq()
    # time.sleep(2)
    # revive_rabbitmq()

    while True:
        result = instance.query('SELECT count(DISTINCT key) FROM test.view')
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.consumer_reconnect;
    ''')

    assert int(result) == messages_num, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_commit_on_block_write(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        DROP TABLE IF EXISTS test.rabbitmq;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'block',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'block',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    cancel = threading.Event()

    i = [0]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(101):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            for message in messages:
                channel.basic_publish(exchange='block', routing_key='', body=message)

    rabbitmq_thread = threading.Thread(target=produce)
    rabbitmq_thread.start()

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        time.sleep(1)

    cancel.set()

    instance.query('DETACH TABLE test.rabbitmq;')

    while int(instance.query("SELECT count() FROM system.tables WHERE database='test' AND name='rabbitmq'")) == 1:
        time.sleep(1)

    instance.query('ATTACH TABLE test.rabbitmq;')

    while int(instance.query('SELECT uniqExact(key) FROM test.view')) < i[0]:
        time.sleep(1)

    result = int(instance.query('SELECT count() == uniqExact(key) FROM test.view'))

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    rabbitmq_thread.join()
    connection.close()

    assert result == 1, 'Messages from RabbitMQ get duplicated!'


def test_rabbitmq_no_connection_at_startup_1(rabbitmq_cluster):
    # no connection when table is initialized
    rabbitmq_cluster.pause_container('rabbitmq1')
    instance.query_and_get_error('''
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_num_consumers = '5',
                     rabbitmq_row_delimiter = '\\n';
    ''')
    rabbitmq_cluster.unpause_container('rabbitmq1')


def test_rabbitmq_no_connection_at_startup_2(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_num_consumers = '5',
                     rabbitmq_row_delimiter = '\\n';
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
    ''')
    instance.query("DETACH TABLE test.cs")
    rabbitmq_cluster.pause_container('rabbitmq1')
    instance.query("ATTACH TABLE test.cs")
    rabbitmq_cluster.unpause_container('rabbitmq1')

    messages_num = 1000
    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    for i in range(messages_num):
        message = json.dumps({'key': i, 'value': i})
        channel.basic_publish(exchange='cs', routing_key='', body=message,
                properties=pika.BasicProperties(delivery_mode=2, message_id=str(i)))
    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.cs;
    ''')

    assert int(result) == messages_num, 'ClickHouse lost some messages: {}'.format(result)


def test_rabbitmq_format_factory_settings(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.format_settings (
            id String, date DateTime
        ) ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'format_settings',
                     rabbitmq_format = 'JSONEachRow',
                     date_time_input_format = 'best_effort';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message = json.dumps({"id":"format_settings_test","date":"2021-01-19T14:42:33.1829214Z"})
    expected = instance.query('''SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))''')

    channel.basic_publish(exchange='format_settings', routing_key='', body=message)
    result = ''
    while True:
        result = instance.query('SELECT date FROM test.format_settings')
        if result == expected:
            break;

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (
            id String, date DateTime
        ) ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        ''')

    channel.basic_publish(exchange='format_settings', routing_key='', body=message)
    result = ''
    while True:
        result = instance.query('SELECT date FROM test.view')
        if result == expected:
            break;

    connection.close()
    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.format_settings;
    ''')

    assert(result == expected)


def test_rabbitmq_vhost(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq_vhost (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'vhost',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_vhost = '/'
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(exchange='vhost', routing_key='', body=json.dumps({'key': 1, 'value': 2}))
    connection.close()
    while True:
        result = instance.query('SELECT * FROM test.rabbitmq_vhost ORDER BY key', ignore_error=True)
        if result == "1\t2\n":
            break


def test_rabbitmq_drop_table_properly(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq_drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'drop',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue_drop'
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='drop', routing_key='', body=json.dumps({'key': 1, 'value': 2}))
    while True:
        result = instance.query('SELECT * FROM test.rabbitmq_drop ORDER BY key', ignore_error=True)
        if result == "1\t2\n":
            break

    exists = channel.queue_declare(queue='rabbit_queue_drop', passive=True)
    assert(exists)

    instance.query("DROP TABLE test.rabbitmq_drop")
    time.sleep(30)

    try:
        exists = channel.queue_declare(callback, queue='rabbit_queue_drop', passive=True)
    except Exception as e:
        exists = False

    assert(not exists)


def test_rabbitmq_queue_settings(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq_settings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'rabbit_exchange',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue_settings',
                     rabbitmq_queue_settings_list = 'x-max-length=10,x-overflow=reject-publish'
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for i in range(50):
        channel.basic_publish(exchange='rabbit_exchange', routing_key='', body=json.dumps({'key': 1, 'value': 2}))
    connection.close()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq_settings;
        ''')

    time.sleep(5)

    result = instance.query('SELECT count() FROM test.rabbitmq_settings', ignore_error=True)
    while int(result) != 10:
        time.sleep(0.5)
        result = instance.query('SELECT count() FROM test.view', ignore_error=True)

    instance.query('DROP TABLE test.rabbitmq_settings')

    # queue size is 10, but 50 messages were sent, they will be dropped (setting x-overflow = reject-publish) and only 10 will remain.
    assert(int(result) == 10)


def test_rabbitmq_queue_consume(rabbitmq_cluster):
    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='rabbit_queue', durable=True)

    i = [0]
    messages_num = 1000
    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        messages = []
        for _ in range(messages_num):
            message = json.dumps({'key': i[0], 'value': i[0]})
            channel.basic_publish(exchange='', routing_key='rabbit_queue', body=message)
            i[0] += 1

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query('''
        CREATE TABLE test.rabbitmq_queue (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue',
                     rabbitmq_queue_consume = 1;

        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq_queue;
        ''')

    result = ''
    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)

    for thread in threads:
        thread.join()

    instance.query('DROP TABLE test.rabbitmq_queue')


def test_rabbitmq_bad_args(rabbitmq_cluster):
    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters(rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='f', exchange_type='fanout')
    instance.query_and_get_error('''
        CREATE TABLE test.drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'f',
                     rabbitmq_format = 'JSONEachRow';
    ''')


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
