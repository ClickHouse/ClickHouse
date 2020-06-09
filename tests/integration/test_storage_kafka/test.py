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
import kafka.errors
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
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

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/kafka.xml', 'configs/log_conf.xml' ],
                                with_kafka=True,
                                with_zookeeper=True,
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
#    print ("Produced {} messages for topic {}".format(len(messages), topic))


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
    instance.query('DROP TABLE IF EXISTS test.kafka')


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
def test_kafka_consumer_hang(kafka_cluster):

    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang',
                     kafka_group_name = 'consumer_hang',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 8,
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = Memory();
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        ''')

    time.sleep(10)
    instance.query('SELECT * FROM test.view')

    # This should trigger heartbeat fail,
    # which will trigger REBALANCE_IN_PROGRESS,
    # and which can lead to consumer hang.
    kafka_cluster.pause_container('kafka1')
    time.sleep(0.5)
    kafka_cluster.unpause_container('kafka1')

    # print("Attempt to drop")
    instance.query('DROP TABLE test.kafka')

    #kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # original problem appearance was a sequence of the following messages in librdkafka logs:
    # BROKERFAIL -> |ASSIGN| -> REBALANCE_IN_PROGRESS -> "waiting for rebalance_cb" (repeated forever)
    # so it was waiting forever while the application will execute queued rebalance callback

    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert int(instance.query("select count() from system.processes where position(lower(query),'dr'||'op')>0")) == 0

@pytest.mark.timeout(180)
def test_kafka_consumer_hang2(kafka_cluster):

    instance.query('''
        DROP TABLE IF EXISTS test.kafka;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_group_name = 'consumer_hang2',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_group_name = 'consumer_hang2',
                     kafka_format = 'JSONEachRow';
        ''')

    # first consumer subscribe the topic, try to poll some data, and go to rest
    instance.query('SELECT * FROM test.kafka')

    # second consumer do the same leading to rebalance in the first
    # consumer, try to poll some data
    instance.query('SELECT * FROM test.kafka2')

#echo 'SELECT * FROM test.kafka; SELECT * FROM test.kafka2; DROP TABLE test.kafka;' | clickhouse client -mn &
#    kafka_cluster.open_bash_shell('instance')

    # first consumer has pending rebalance callback unprocessed (no poll after select)
    # one of those queries was failing because of
    # https://github.com/edenhill/librdkafka/issues/2077
    # https://github.com/edenhill/librdkafka/issues/2898
    instance.query('DROP TABLE test.kafka')
    instance.query('DROP TABLE test.kafka2')


    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert int(instance.query("select count() from system.processes where position(lower(query),'dr'||'op')>0")) == 0


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
def test_kafka_select_empty(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'empty',
                     kafka_group_name = 'empty',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        ''')

    assert int(instance.query('SELECT count() FROM test.kafka')) == 0


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
def test_kafka_materialized_view_with_subquery(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mvsq',
                     kafka_group_name = 'mvsq',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.kafka);
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('mvsq', messages)

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


@pytest.mark.timeout(240)
def test_kafka_produce_consume(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert2',
                     kafka_group_name = 'insert2',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
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


@pytest.mark.timeout(180)
def test_kafka_virtual_columns2(kafka_cluster):

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topic_list = []
    topic_list.append(NewTopic(name="virt2_0", num_partitions=2, replication_factor=1))
    topic_list.append(NewTopic(name="virt2_1", num_partitions=2, replication_factor=1))

    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        CREATE TABLE test.kafka (value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt2_0,virt2_1',
                     kafka_group_name = 'virt2',
                     kafka_format = 'JSONEachRow';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp) FROM test.kafka;
        ''')

    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    producer.send(topic='virt2_0', value=json.dumps({'value': 1}), partition=0, key='k1', timestamp_ms=1577836801000)
    producer.send(topic='virt2_0', value=json.dumps({'value': 2}), partition=0, key='k2', timestamp_ms=1577836802000)
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_0', value=json.dumps({'value': 3}), partition=1, key='k3', timestamp_ms=1577836803000)
    producer.send(topic='virt2_0', value=json.dumps({'value': 4}), partition=1, key='k4', timestamp_ms=1577836804000)
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_1', value=json.dumps({'value': 5}), partition=0, key='k5', timestamp_ms=1577836805000)
    producer.send(topic='virt2_1', value=json.dumps({'value': 6}), partition=0, key='k6', timestamp_ms=1577836806000)
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_1', value=json.dumps({'value': 7}), partition=1, key='k7', timestamp_ms=1577836807000)
    producer.send(topic='virt2_1', value=json.dumps({'value': 8}), partition=1, key='k8', timestamp_ms=1577836808000)
    producer.flush()

    time.sleep(10)

    result = instance.query("SELECT * FROM test.view ORDER BY value", ignore_error=True)

    expected = '''\
1	k1	virt2_0	0	0	1577836801
2	k2	virt2_0	0	1	1577836802
3	k3	virt2_0	1	0	1577836803
4	k4	virt2_0	1	1	1577836804
5	k5	virt2_1	0	0	1577836805
6	k6	virt2_1	0	1	1577836806
7	k7	virt2_1	1	0	1577836807
8	k8	virt2_1	1	1	1577836808
'''

    assert TSV(result) == TSV(expected)



@pytest.mark.timeout(240)
def test_kafka_produce_key_timestamp(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka_writer (key UInt64, value UInt64, _key String, _timestamp DateTime)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert3',
                     kafka_group_name = 'insert3',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';

        CREATE TABLE test.kafka (key UInt64, value UInt64, inserted_key String, inserted_timestamp DateTime)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert3',
                     kafka_group_name = 'insert3',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value, inserted_key, toUnixTimestamp(inserted_timestamp), _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp) FROM test.kafka;
    ''')

    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(1,1,'k1',1577836801))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(2,2,'k2',1577836802))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({})),({},{},'{}',toDateTime({}))".format(3,3,'k3',1577836803,4,4,'k4',1577836804))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(5,5,'k5',1577836805))

    time.sleep(10)

    result = instance.query("SELECT * FROM test.view ORDER BY value", ignore_error=True)

    # print(result)

    expected = '''\
1	1	k1	1577836801	k1	insert3	0	0	1577836801
2	2	k2	1577836802	k2	insert3	0	1	1577836802
3	3	k3	1577836803	k3	insert3	0	2	1577836803
4	4	k4	1577836804	k4	insert3	0	3	1577836804
5	5	k5	1577836805	k5	insert3	0	4	1577836805
'''

    assert TSV(result) == TSV(expected)



@pytest.mark.timeout(600)
def test_kafka_flush_by_time(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush_by_time',
                     kafka_group_name = 'flush_by_time',
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

    def produce():
        while not cancel.is_set():
            messages = []
            messages.append(json.dumps({'key': 0, 'value': 0}))
            kafka_produce('flush_by_time', messages)
            time.sleep(1)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    time.sleep(18)

    result = instance.query('SELECT count() FROM test.view')

    print(result)
    cancel.set()
    kafka_thread.join()

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # 40 = 2 flushes (7.5 sec), 15 polls each, about 1 mgs per 1.5 sec
    assert int(result) > 12, 'Messages from kafka should be flushed at least every stream_flush_interval_ms!'


@pytest.mark.timeout(600)
def test_kafka_flush_by_block_size(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush_by_block_size',
                     kafka_group_name = 'flush_by_block_size',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';

        SELECT * FROM test.kafka;

        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for _ in range(101):
        messages.append(json.dumps({'key': 0, 'value': 0}))
    kafka_produce('flush_by_block_size', messages)

    time.sleep(1)

    # TODO: due to https://github.com/ClickHouse/ClickHouse/issues/11216
    # second flush happens earlier than expected, so we have 2 parts here instead of one
    # flush by block size works correctly, so the feature checked by the test is working correctly
    result = instance.query("SELECT count() FROM test.view WHERE _part='all_1_1_0'")
    # print(result)

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert int(result) == 100, 'Messages from kafka should be flushed at least every stream_flush_interval_ms!'


@pytest.mark.timeout(600)
def test_kafka_lot_of_partitions_partial_commit_of_bulk(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

    topic_list = []
    topic_list.append(NewTopic(name="topic_with_multiple_partitions2", num_partitions=10, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'topic_with_multiple_partitions2',
                     kafka_group_name = 'topic_with_multiple_partitions2',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 211;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    count = 0
    for dummy_msg in range(1000):
        rows = []
        for dummy_row in range(random.randrange(3,10)):
            count = count + 1
            rows.append(json.dumps({'key': count, 'value': count}))
        messages.append("\n".join(rows))
    kafka_produce('topic_with_multiple_partitions2', messages)

    time.sleep(30)

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(count) )

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

@pytest.mark.timeout(1200)
def test_kafka_rebalance(kafka_cluster):

    NUMBER_OF_CONSURRENT_CONSUMERS=11

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

   # kafka_cluster.open_bash_shell('instance')

    #time.sleep(2)

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topic_list = []
    topic_list.append(NewTopic(name="topic_with_multiple_partitions", num_partitions=11, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    cancel = threading.Event()

    msg_index = [0]
    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(59):
                messages.append(json.dumps({'key': msg_index[0], 'value': msg_index[0]}))
                msg_index[0] += 1
            kafka_produce('topic_with_multiple_partitions', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
        table_name = 'kafka_consumer{}'.format(consumer_index)
        print("Setting up {}".format(table_name))

        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = 'topic_with_multiple_partitions',
                        kafka_group_name = 'rebalance_test_group',
                        kafka_format = 'JSONEachRow',
                        kafka_max_block_size = 33;
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
                SELECT
                key,
                value,
                _topic,
                _key,
                _offset,
                _partition,
                _timestamp,
                '{0}' as _consumed_by
            FROM test.{0};
        '''.format(table_name))
        # kafka_cluster.open_bash_shell('instance')
        while int(instance.query("SELECT count() FROM test.destination WHERE _consumed_by='{}'".format(table_name))) == 0:
            print("Waiting for test.kafka_consumer{} to start consume".format(consumer_index))
            time.sleep(1)

    cancel.set()

    # I leave last one working by intent (to finish consuming after all rebalances)
    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS-1):
        print("Dropping test.kafka_consumer{}".format(consumer_index))
        instance.query('DROP TABLE IF EXISTS test.kafka_consumer{}'.format(consumer_index))
        while int(instance.query("SELECT count() FROM system.tables WHERE database='test' AND name='kafka_consumer{}'".format(consumer_index))) == 1:
            time.sleep(1)

    # print(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))
    # kafka_cluster.open_bash_shell('instance')

    while 1:
        messages_consumed = int(instance.query('SELECT uniqExact(key) FROM test.destination'))
        if messages_consumed >= msg_index[0]:
            break
        time.sleep(1)
        print("Waiting for finishing consuming (have {}, should be {})".format(messages_consumed,msg_index[0]))

    print(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))

    # Some queries to debug...
    # SELECT * FROM test.destination where key in (SELECT key FROM test.destination group by key having count() <> 1)
    # select number + 1 as key from numbers(4141) left join test.destination using (key) where  test.destination.key = 0;
    # SELECT * FROM test.destination WHERE key between 2360 and 2370 order by key;
    # select _partition from test.destination group by _partition having count() <> max(_offset) + 1;
    # select toUInt64(0) as _partition, number + 1 as _offset from numbers(400) left join test.destination using (_partition,_offset) where test.destination.key = 0 order by _offset;
    # SELECT * FROM test.destination WHERE _partition = 0 and _offset between 220 and 240 order by _offset;

    # CREATE TABLE test.reference (key UInt64, value UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:19092',
    #             kafka_topic_list = 'topic_with_multiple_partitions',
    #             kafka_group_name = 'rebalance_test_group_reference',
    #             kafka_format = 'JSONEachRow',
    #             kafka_max_block_size = 100000;
    #
    # CREATE MATERIALIZED VIEW test.reference_mv Engine=Log AS
    #     SELECT  key, value, _topic,_key,_offset, _partition, _timestamp, 'reference' as _consumed_by
    # FROM test.reference;
    #
    # select * from test.reference_mv left join test.destination using (key,_topic,_offset,_partition) where test.destination._consumed_by = '';

    result = int(instance.query('SELECT count() == uniqExact(key) FROM test.destination'))

    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
        print("kafka_consumer{}".format(consumer_index))
        table_name = 'kafka_consumer{}'.format(consumer_index)
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
        '''.format(table_name))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    kafka_thread.join()

    assert result == 1, 'Messages from kafka get duplicated!'

@pytest.mark.timeout(1200)
def test_kafka_no_holes_when_write_suffix_failed(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce('no_holes_when_write_suffix_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'no_holes_when_write_suffix_failed',
                     kafka_group_name = 'no_holes_when_write_suffix_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = ReplicatedMergeTree('/clickhouse/kafkatest/tables/no_holes_when_write_suffix_failed', 'node1')
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(1);
    ''')
    # the tricky part here is that disconnect should happen after write prefix, but before write suffix
    # so i use sleepEachRow
    with PartitionManager() as pm:
        time.sleep(12)
        pm.drop_instance_zk_connections(instance)
        time.sleep(20)
        pm.heal_all

    # connection restored and it will take a while until next block will be flushed
    # it takes years on CI :\
    time.sleep(90)

    # as it's a bit tricky to hit the proper moment - let's check in logs if we did it correctly
    assert instance.contains_in_log("ZooKeeper session has been expired.: while write prefix to view")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('22\t22\t22')


@pytest.mark.timeout(120)
def test_exception_from_destructor(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_format = 'JSONEachRow';
    ''')
    instance.query_and_get_error('''
        SELECT * FROM test.kafka;
    ''')
    instance.query('''
        DROP TABLE test.kafka;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_format = 'JSONEachRow';
    ''')
    instance.query('''
        DROP TABLE test.kafka;
    ''')

    #kafka_cluster.open_bash_shell('instance')
    assert TSV(instance.query('SELECT 1')) == TSV('1')


@pytest.mark.timeout(120)
def test_commits_of_unprocessed_messages_on_drop(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(1)]
    kafka_produce('commits_of_unprocessed_messages_on_drop', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.destination AS
            SELECT
            key,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    ''')

    while int(instance.query("SELECT count() FROM test.destination")) == 0:
        print("Waiting for test.kafka_consumer to start consume")
        time.sleep(1)

    cancel = threading.Event()

    i = [2]
    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(113):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            kafka_produce('commits_of_unprocessed_messages_on_drop', messages)
            time.sleep(1)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()
    time.sleep(12)

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 10000;
    ''')

    cancel.set()
    time.sleep(15)

    #kafka_cluster.open_bash_shell('instance')
    # SELECT key, _timestamp, _offset FROM test.destination where runningDifference(key) <> 1 ORDER BY key;

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.destination')
    print(result)

    instance.query('''
        DROP TABLE test.kafka_consumer;
        DROP TABLE test.destination;
    ''')

    kafka_thread.join()
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(i[0]-1)), 'Missing data!'



@pytest.mark.timeout(120)
def test_bad_reschedule(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(20000)]
    kafka_produce('test_bad_reschedule', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'test_bad_reschedule',
                    kafka_group_name = 'test_bad_reschedule',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

        CREATE MATERIALIZED VIEW test.destination Engine=Log AS
        SELECT
            key,
            now() as consume_ts,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    ''')

    while int(instance.query("SELECT count() FROM test.destination")) < 20000:
        print("Waiting for consume")
        time.sleep(1)

    assert int(instance.query("SELECT max(consume_ts) - min(consume_ts) FROM test.destination")) < 8


@pytest.mark.timeout(1200)
def test_kafka_duplicates_when_commit_failed(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce('duplicates_when_commit_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'duplicates_when_commit_failed',
                     kafka_group_name = 'duplicates_when_commit_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(0.5);
    ''')

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    time.sleep(12) # 5-6 sec to connect to kafka, do subscription, and fetch 20 rows, another 10 sec for MV, after that commit should happen

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    kafka_cluster.pause_container('kafka1')
    # that timeout it VERY important, and picked after lot of experiments
    # when too low (<30sec) librdkafka will not report any timeout (alternative is to decrease the default session timeouts for librdkafka)
    # when too high (>50sec) broker will decide to remove us from the consumer group, and will start answering "Broker: Unknown member"
    time.sleep(40)

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    kafka_cluster.unpause_container('kafka1')

    #kafka_cluster.open_bash_shell('instance')

    # connection restored and it will take a while until next block will be flushed
    # it takes years on CI :\
    time.sleep(30)

    # as it's a bit tricky to hit the proper moment - let's check in logs if we did it correctly
    assert instance.contains_in_log("Local: Waiting for coordinator")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('22\t22\t22')



if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
