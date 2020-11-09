import os.path as p
import random
import threading
import time
import pytest

from random import randrange
import pika
from sys import getdefaultencoding

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess

from google.protobuf.internal.encoder import _VarintBytes

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/rabbitmq.xml','configs/log_conf.xml'],
                                with_rabbitmq=True)
rabbitmq_id = ''


# Helpers

def check_rabbitmq_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          'rabbitmqctl',
                          'await_startup'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def enable_consistent_hash_plugin():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          "rabbitmq-plugins", "enable", "rabbitmq_consistent_hash_exchange"),
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


def wait_rabbitmq_plugin_enabled(max_retries=50):
    retries = 0
    while True:
        if enable_consistent_hash_plugin():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "RabbitMQ plugin is not available"
            print("Waiting for plugin")
            time.sleep(1)


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
    wait_rabbitmq_plugin_enabled()
    print("RabbitMQ is available - running test")
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests

@pytest.mark.timeout(180)
def test_rabbitmq_select_from_new_syntax_table(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'new',
                     rabbitmq_exchange_name = 'clickhouse-exchange',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))

    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='new', body=message)

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='new', body=message)

    connection.close()

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_select_from_old_syntax_table(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ('rabbitmq1:5672', 'old', 'clickhouse-exchange', 'JSONEachRow', '\\n');
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))

    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='old', body=message)

    connection.close()

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_select_empty(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'empty',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    assert int(instance.query('SELECT count() FROM test.rabbitmq')) == 0


@pytest.mark.timeout(180)
def test_rabbitmq_json_without_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'json',
                     rabbitmq_exchange_name = 'clickhouse-exchange',
                     rabbitmq_format = 'JSONEachRow'
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'

    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='json', body=message)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='json', body=message)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_csv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'csv',
                     rabbitmq_exchange_name = 'clickhouse-exchange',
                     rabbitmq_format = 'CSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='csv', body=message)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break


    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_tsv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'tsv',
                     rabbitmq_exchange_name = 'clickhouse-exchange',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='tsv', body=message)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_materialized_view(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='mv', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if (rabbitmq_check_result(result)):
            break;

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_materialized_view_with_subquery(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mvsq',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.rabbitmq);
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='mvsq', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if rabbitmq_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close();
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_many_materialized_views(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mmv',
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
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='mmv', body=message)

    while True:
        result1 = instance.query('SELECT * FROM test.view1')
        result2 = instance.query('SELECT * FROM test.view2')
        if rabbitmq_check_result(result1) and rabbitmq_check_result(result2):
            break

    instance.query('''
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    ''')

    rabbitmq_check_result(result1, True)
    rabbitmq_check_result(result2, True)


@pytest.mark.timeout(240)
def test_rabbitmq_big_message(rabbitmq_cluster):
    # Create batchs of messages of size ~100Kb
    rabbitmq_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(rabbitmq_messages)]

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'big',
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    for message in messages:
        channel.basic_publish(exchange='clickhouse-exchange', routing_key='big', body=message)

    while True:
        result = instance.query('SELECT count() FROM test.view')
        print("Result", result, "Expected", batch_messages * rabbitmq_messages)
        if int(result) == batch_messages * rabbitmq_messages:
            break

    connection.close()
    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == rabbitmq_messages*batch_messages, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_sharding_between_channels_publish(rabbitmq_cluster):

    NUM_CHANNELS = 5

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_consumers = 5,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    time.sleep(1)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        key = str(randrange(1, NUM_CHANNELS))
        for message in messages:
            channel.basic_publish(exchange='clickhouse-exchange', routing_key=key, body=message)
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        print("Result", result, "Expected", messages_num * threads_num)
        if int(result) == messages_num * threads_num:
            break

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_sharding_between_queues_publish(rabbitmq_cluster):

    NUM_QUEUES = 4

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_queues = 4,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    time.sleep(1)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        key = str(randrange(1, NUM_QUEUES))
        for message in messages:
            channel.basic_publish(exchange='clickhouse-exchange', routing_key=key, body=message)
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_sharding_between_channels_and_queues_publish(rabbitmq_cluster):

    NUM_CONSUMERS = 10
    NUM_QUEUES = 2

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_queues = 2,
                     rabbitmq_num_consumers = 10,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    time.sleep(1)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        key = str(randrange(1, NUM_QUEUES * NUM_CONSUMERS))
        for message in messages:
            channel.basic_publish(exchange='clickhouse-exchange', routing_key=key, body=message)
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_read_only_combo(rabbitmq_cluster):

    NUM_MV = 5;
    NUM_CONSUMERS = 4

    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_consumers = 4,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    for mv_id in range(NUM_MV):
        table_name = 'view{}'.format(mv_id)
        print("Setting up {}".format(table_name))

        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.{0} AS
                SELECT * FROM test.rabbitmq;
        '''.format(table_name))

    time.sleep(2)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='clickhouse-exchange', exchange_type='fanout')

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        key = str(randrange(1, NUM_CONSUMERS))
        for message in messages:
            channel.basic_publish(exchange='clickhouse-exchange', routing_key=key, body=message)
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
        for view in range(NUM_MV):
            result += int(instance.query('SELECT count() FROM test.view{0}'.format(view)))
        if int(result) == messages_num * threads_num * NUM_MV:
            break
        time.sleep(1)

    for thread in threads:
        thread.join()

    for mv_id in range(NUM_MV):
        table_name = 'view{}'.format(mv_id)
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
        '''.format(table_name))


    assert int(result) == messages_num * threads_num * NUM_MV, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(240)
def test_rabbitmq_insert(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert',
                     rabbitmq_routing_key_list = 'insert1',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    consumer.exchange_declare(exchange='insert_rabbitmq_direct', exchange_type='direct')
    result = consumer.queue_declare(queue='')
    queue_name = result.method.queue
    consumer.queue_bind(exchange='insert_rabbitmq_direct', queue=queue_name, routing_key='insert1')

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

    consumer.basic_qos(prefetch_count=50)
    consumer.basic_consume(onReceived, queue_name)
    consumer.start_consuming()
    consumer_connection.close()

    result = '\n'.join(insert_messages)
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(240)
def test_rabbitmq_many_inserts(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.rabbitmq_many;
        DROP TABLE IF EXISTS test.view_many;
        DROP TABLE IF EXISTS test.consumer_many;
        CREATE TABLE test.rabbitmq_many (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.rabbitmq_many;
    ''')

    messages_num = 1000
    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

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
    threads_num = 20
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view_many')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE IF EXISTS test.rabbitmq_many;
        DROP TABLE IF EXISTS test.consumer_many;
        DROP TABLE IF EXISTS test.view_many;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(240)
def test_rabbitmq_sharding_between_channels_and_queues_insert(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view_sharding;
        DROP TABLE IF EXISTS test.consumer_sharding;
        CREATE TABLE test.rabbitmq_sharding (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_consumers = 5,
                     rabbitmq_num_queues = 2,
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view_sharding (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_sharding TO test.view_sharding AS
            SELECT * FROM test.rabbitmq_sharding;
    ''')

    messages_num = 10000
    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

        while True:
            try:
                instance.query("INSERT INTO test.rabbitmq_sharding VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 20
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view_sharding')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE IF EXISTS test.rabbitmq_sharding;
        DROP TABLE IF EXISTS test.consumer_sharding;
        DROP TABLE IF EXISTS test.view_sharding;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_overloaded_insert(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        CREATE TABLE test.rabbitmq_overload (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_consumers = 10,
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.rabbitmq_overload;
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
        print("Result", int(result), "Expected", messages_num * threads_num)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE IF EXISTS test.rabbitmq_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        DROP TABLE IF EXISTS test.view_overload;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_direct_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print("Setting up table {}".format(consumer_id))
        instance.query('''
            DROP TABLE IF EXISTS test.direct_exchange_{0};
            DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
            CREATE TABLE test.direct_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 5,
                         rabbitmq_exchange_name = 'direct_exchange_testing',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_routing_key_list = 'direct_{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.direct_exchange_{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.direct_exchange_{0};
        '''.format(consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange_testing', exchange_type='direct')

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
            DROP TABLE IF EXISTS test.direct_exchange_{0};
            DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_fanout_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print("Setting up table {}".format(consumer_id))
        instance.query('''
            DROP TABLE IF EXISTS test.fanout_exchange_{0};
            DROP TABLE IF EXISTS test.fanout_exchange_{0}_mv;
            CREATE TABLE test.fanout_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 5,
                         rabbitmq_routing_key_list = 'key_{0}',
                         rabbitmq_exchange_name = 'fanout_exchange_testing',
                         rabbitmq_exchange_type = 'fanout',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.fanout_exchange_{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.fanout_exchange_{0};
        '''.format(consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='fanout_exchange_testing', exchange_type='fanout')

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    key_num = 0
    for message in messages:
        mes_id = str(randrange(10))
        channel.basic_publish(
                exchange='fanout_exchange_testing', routing_key='',
                properties=pika.BasicProperties(message_id=mes_id), body=message)

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables:
            break

    for consumer_id in range(num_tables):
        instance.query('''
            DROP TABLE IF EXISTS test.fanout_exchange_{0};
            DROP TABLE IF EXISTS test.fanout_exchange_{0}_mv;
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_topic_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 5
    for consumer_id in range(num_tables):
        print("Setting up table {}".format(consumer_id))
        instance.query('''
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 5,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.topic_exchange_{0};
        '''.format(consumer_id))

    for consumer_id in range(num_tables):
        print("Setting up table {}".format(num_tables + consumer_id))
        instance.query('''
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 4,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.logs',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.topic_exchange_{0};
        '''.format(num_tables + consumer_id))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='topic_exchange_testing', exchange_type='topic')

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
    for message in messages:
        mes_id = str(randrange(10))
        channel.basic_publish(
                exchange='topic_exchange_testing', routing_key=key,
                properties=pika.BasicProperties(message_id=mes_id), body=message)

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables + messages_num * num_tables:
            break

    for consumer_id in range(num_tables * 2):
        instance.query('''
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * num_tables + messages_num * num_tables, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_hash_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables = 4
    for consumer_id in range(num_tables):
        table_name = 'rabbitmq_consumer{}'.format(consumer_id)
        print("Setting up {}".format(table_name))
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 10,
                         rabbitmq_exchange_type = 'consistent_hash',
                         rabbitmq_exchange_name = 'hash_exchange_testing',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.{0};
        '''.format(table_name))

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='hash_exchange_testing', exchange_type='x-consistent-hash')
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1
        for message in messages:
            key = str(randrange(10))
            channel.basic_publish(exchange='hash_exchange_testing', routing_key=key, body=message)
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
        if int(result) == messages_num * threads_num:
            break

    for consumer_id in range(num_tables):
        table_name = 'rabbitmq_consumer{}'.format(consumer_id)
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
        '''.format(table_name))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_multiple_bindings(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    instance.query('''
        DROP TABLE IF EXISTS test.bindings_1;
        DROP TABLE IF EXISTS test.bindings_1_mv;
        CREATE TABLE test.bindings_1 (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_num_consumers = 5,
                     rabbitmq_num_queues = 2,
                     rabbitmq_exchange_name = 'multiple_bindings_testing',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'key1,key2,key3,key4,key5',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.bindings_1_mv TO test.destination AS
            SELECT * FROM test.bindings_1;
    ''')

    # in case num_consumers and num_queues are not set - multiple bindings are implemented differently, so test them too
    instance.query('''
        DROP TABLE IF EXISTS test.bindings_2;
        DROP TABLE IF EXISTS test.bindings_2_mv;
        CREATE TABLE test.bindings_2 (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'multiple_bindings_testing',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'key1,key2,key3,key4,key5',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.bindings_2_mv TO test.destination AS
            SELECT * FROM test.bindings_2;
    ''')

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='multiple_bindings_testing', exchange_type='direct')

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({'key': i[0], 'value': i[0]}))
            i[0] += 1

        keys = ['key1', 'key2', 'key3', 'key4', 'key5']

        for key in keys:
            for message in messages:
                mes_id = str(randrange(10))
                channel.basic_publish(exchange='multiple_bindings_testing', routing_key=key,
                    properties=pika.BasicProperties(message_id=mes_id), body=message)

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
        if int(result) == messages_num * threads_num * 5 * 2:
            break

    for thread in threads:
        thread.join()

    instance.query('''
        DROP TABLE IF EXISTS test.bindings_1;
        DROP TABLE IF EXISTS test.bindings_2;
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * threads_num * 5 * 2, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(420)
def test_rabbitmq_headers_exchange(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64,
            _consumed_by LowCardinality(String))
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    num_tables_to_receive = 3
    for consumer_id in range(num_tables_to_receive):
        print("Setting up table {}".format(consumer_id))
        instance.query('''
            DROP TABLE IF EXISTS test.headers_exchange_{0};
            DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
            CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 4,
                         rabbitmq_exchange_name = 'headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2020',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
                SELECT key, value, '{0}' as _consumed_by FROM test.headers_exchange_{0};
        '''.format(consumer_id))

    num_tables_to_ignore = 2
    for consumer_id in range(num_tables_to_ignore):
        print("Setting up table {}".format(consumer_id + num_tables_to_receive))
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
                SELECT key, value, '{0}' as _consumed_by FROM test.headers_exchange_{0};
        '''.format(consumer_id + num_tables_to_receive))

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='headers_exchange_testing', exchange_type='headers')

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({'key': i[0], 'value': i[0]}))
        i[0] += 1

    fields={}
    fields['format']='logs'
    fields['type']='report'
    fields['year']='2020'

    key_num = 0
    for message in messages:
        mes_id = str(randrange(10))
        channel.basic_publish(exchange='headers_exchange_testing', routing_key='',
                properties=pika.BasicProperties(headers=fields, message_id=mes_id), body=message)

    connection.close()

    while True:
        result = instance.query('SELECT count() FROM test.destination')
        time.sleep(1)
        if int(result) == messages_num * num_tables_to_receive:
            break

    for consumer_id in range(num_tables_to_receive + num_tables_to_ignore):
        instance.query('''
            DROP TABLE IF EXISTS test.direct_exchange_{0};
            DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
        '''.format(consumer_id))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    assert int(result) == messages_num * num_tables_to_receive, 'ClickHouse lost some messages: {}'.format(result)


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
