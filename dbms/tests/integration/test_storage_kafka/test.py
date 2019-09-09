import os.path as p
import random
import threading
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException

import json
import subprocess
import kafka.errors
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from google.protobuf.internal.encoder import _VarintBytes

"""
protoc --version
libprotoc 3.0.0

# to create kafka_pb2.py
protoc --python_out=. kafka.proto
"""
import kafka_pb2


# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.
# TODO: modify tests to respect `skip_broken_messages` setting.

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/kafka.xml'],
                                with_kafka=True,
                                clickhouse_path_dir='clickhouse_path')
kafka_id = ''


# Helpers

def check_kafka_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          kafka_id,
                          '/usr/bin/kafka-broker-api-versions',
                          '--bootstrap-server',
                          'INSIDE://localhost:9092'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def wait_kafka_is_available(max_retries=50):
    retries = 0
    while True:
        if check_kafka_is_available():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "Kafka is not available"
            print("Waiting for Kafka to start up")
            time.sleep(1)


def kafka_produce(topic, messages, timestamp=None):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()
    print ("Produced {} messages for topic {}".format(len(messages), topic))


def kafka_consume(topic):
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", auto_offset_reset="earliest")
    consumer.subscribe(topics=(topic))
    for toppar, messages in consumer.poll(5000).items():
        if toppar.topic == topic:
            for message in messages:
                yield message.value
    consumer.unsubscribe()
    consumer.close()


def kafka_produce_protobuf_messages(topic, start_index, num_messages):
    data = ''
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    producer.send(topic=topic, value=data)
    producer.flush()
    print("Produced {} messages for topic {}".format(num_messages, topic))


# Since everything is async and shaky when receiving messages from Kafka,
# we may want to try and check results multiple times in a loop.
def  kafka_check_result(result, check=False, ref_file='test_kafka_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# Fixtures

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        global kafka_id
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print("kafka_id is {}".format(kafka_id))
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP TABLE IF EXISTS test.kafka')
    wait_kafka_is_available()
    print("kafka is available - running test")
    yield  # run test
    instance.query('DROP TABLE test.kafka')


# Tests

@pytest.mark.timeout(180)
def test_kafka_settings_old_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('kafka1:19092', 'old', 'old', 'JSONEachRow', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('old', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'new',
                     kafka_group_name = 'new',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n',
                     kafka_skip_broken_messages = 1;
        ''')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('new', messages)

    # Insert couple of malformed messages.
    kafka_produce('new', ['}{very_broken_message,'])
    kafka_produce('new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('new', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_csv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv',
                     kafka_group_name = 'csv',
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n';
        ''')

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))
    kafka_produce('csv', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_tsv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'tsv',
                     kafka_group_name = 'tsv',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        ''')

    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))
    kafka_produce('tsv', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_json_without_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'json',
                     kafka_group_name = 'json',
                     kafka_format = 'JSONEachRow';
        ''')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('json', [messages])

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('json', [messages])

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_protobuf(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb',
                     kafka_group_name = 'pb',
                     kafka_format = 'Protobuf',
                     kafka_schema = 'kafka.proto:KeyValuePair';
        ''')

    kafka_produce_protobuf_messages('pb', 0, 20)
    kafka_produce_protobuf_messages('pb', 20, 1)
    kafka_produce_protobuf_messages('pb', 21, 29)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_materialized_view(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mv',
                     kafka_group_name = 'mv',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('mv', messages)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if kafka_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_many_materialized_views(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mmv',
                     kafka_group_name = 'mmv',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.kafka;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('mmv', messages)

    while True:
        result1 = instance.query('SELECT * FROM test.view1')
        result2 = instance.query('SELECT * FROM test.view2')
        if kafka_check_result(result1) and kafka_check_result(result2):
            break

    instance.query('''
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    ''')

    kafka_check_result(result1, True)
    kafka_check_result(result2, True)


@pytest.mark.timeout(300)
def test_kafka_flush_on_big_message(kafka_cluster):
    # Create batchs of messages of size ~100Kb
    kafka_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(kafka_messages)]
    kafka_produce('flush', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush',
                     kafka_group_name = 'flush',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 10;
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    received = False
    while not received:
        try:
            offsets = client.list_consumer_group_offsets('flush')
            for topic, offset in offsets.items():
                if topic.topic == 'flush' and offset.offset == kafka_messages:
                    received = True
                    break
        except kafka.errors.GroupCoordinatorNotAvailableError:
            continue

    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == kafka_messages*batch_messages:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == kafka_messages*batch_messages, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(180)
def test_kafka_virtual_columns(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt1',
                     kafka_group_name = 'virt1',
                     kafka_format = 'JSONEachRow';
        ''')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('virt1', [messages], 0)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('virt1', [messages], 0)

    result = ''
    while True:
        result += instance.query('SELECT _key, key, _topic, value, _offset, _partition, _timestamp FROM test.kafka', ignore_error=True)
        if kafka_check_result(result, False, 'test_kafka_virtual1.reference'):
            break

    kafka_check_result(result, True, 'test_kafka_virtual1.reference')


@pytest.mark.timeout(180)
def test_kafka_virtual_columns_with_materialized_view(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt2',
                     kafka_group_name = 'virt2',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64, kafka_key String, topic String, offset UInt64, partition UInt64, timestamp Nullable(DateTime))
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _key as kafka_key, _topic as topic, _offset as offset, _partition as partition, _timestamp as timestamp FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('virt2', messages, 0)

    while True:
        result = instance.query('SELECT kafka_key, key, topic, value, offset, partition, timestamp FROM test.view')
        if kafka_check_result(result, False, 'test_kafka_virtual2.reference'):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True, 'test_kafka_virtual2.reference')


@pytest.mark.timeout(180)
def test_kafka_insert(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert1',
                     kafka_group_name = 'insert1',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
    ''')

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.kafka VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    messages = []
    while True:
        messages.extend(kafka_consume('insert1'))
        if len(messages) == 50:
            break

    result = '\n'.join(messages)
    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_produce_consume(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert2',
                     kafka_group_name = 'insert2',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
    ''')

    messages_num = 10000
    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

        while True:
            try:
                instance.query("INSERT INTO test.kafka VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 16
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(300)
def test_kafka_commit_on_block_write(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'block',
                     kafka_group_name = 'block',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    cancel = threading.Event()

    i = [0]
    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(101):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            kafka_produce('block', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        time.sleep(1)

    cancel.set()

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    while int(instance.query("SELECT count() FROM system.tables WHERE database='test' AND name='kafka'")) == 1:
        time.sleep(1)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'block',
                     kafka_group_name = 'block',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';
    ''')

    while int(instance.query('SELECT uniqExact(key) FROM test.view')) < i[0]:
        time.sleep(1)

    result = int(instance.query('SELECT count() == uniqExact(key) FROM test.view'))

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_thread.join()

    assert result == 1, 'Messages from kafka get duplicated!'


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
