import os.path as p
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import json
import subprocess


# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for mat. view is working.
# TODO: add test for SELECT LIMIT is working.
# TODO: modify tests to respect `skip_broken_messages` setting.

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                main_configs=['configs/kafka.xml'],
                                with_kafka=True)
kafka_id = instance.cluster.kafka_docker_id


# Helpers

def check_kafka_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          kafka_id,
                          '/usr/bin/kafka-broker-api-versions',
                          '--bootstrap-server',
                          'PLAINTEXT://localhost:9092'),
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


def kafka_produce(topic, messages):
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          kafka_id,
                          '/usr/bin/kafka-console-producer',
                          '--broker-list',
                          'localhost:9092',
                          '--topic',
                          topic),
                         stdin=subprocess.PIPE)
    p.communicate(messages)
    p.stdin.close()


def  kafka_check_result(result):
    fpath = p.join(p.dirname(__file__), 'test_kafka_json.reference')
    with open(fpath) as reference:
        assert TSV(result) == TSV(reference)


# Fixtures

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP TABLE IF EXISTS test.kafka')
    wait_kafka_is_available()
    yield  # run test
    instance.query('DROP TABLE test.kafka')


# Tests

def test_kafka_settings_old_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('kafka1:9092', 'old', 'old', 'JSONEachRow', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = ''
    for i in range(50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('old', messages)

    result = instance.query('SELECT * FROM test.kafka')
    kafka_check_result(result)


def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'new',
                kafka_group_name = 'new',
                kafka_format = 'JSONEachRow',
                kafka_row_delimiter = '\\n',
                kafka_skip_broken_messages = 1;
        ''')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('new', messages)

    # Insert couple of malformed messages.
    kafka_produce('new', '}{very_broken_message,\n')
    kafka_produce('new', '}another{very_broken_message,\n')

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('new', messages)

    # Since the broken message breaks the `select`,
    # we'll try to select multiple times.
    result = instance.query('SELECT * FROM test.kafka')
    result += instance.query('SELECT * FROM test.kafka')
    result += instance.query('SELECT * FROM test.kafka')
    kafka_check_result(result)


def test_kafka_csv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'csv',
                kafka_group_name = 'csv',
                kafka_format = 'CSV',
                kafka_row_delimiter = '\\n';
        ''')

    messages = ''
    for i in range(50):
        messages += '{i}, {i}\n'.format(i=i)
    kafka_produce('csv', messages)

    result = instance.query('SELECT * FROM test.kafka')
    kafka_check_result(result)


def test_kafka_tsv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'tsv',
                kafka_group_name = 'tsv',
                kafka_format = 'TSV',
                kafka_row_delimiter = '\\n';
        ''')

    messages = ''
    for i in range(50):
        messages += '{i}\t{i}\n'.format(i=i)
    kafka_produce('tsv', messages)

    result = instance.query('SELECT * FROM test.kafka')
    kafka_check_result(result)


def test_kafka_materialized_view(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'json',
                kafka_group_name = 'json',
                kafka_format = 'JSONEachRow',
                kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = ''
    for i in range(50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('json', messages)

    # Try select multiple times, until we get results
    for i in range(3):
        time.sleep(1)
        result = instance.query('SELECT * FROM test.view')
        if result:
            break
    kafka_check_result(result)

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
