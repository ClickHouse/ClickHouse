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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


def kafka_is_available(kafka_id):
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          kafka_id,
                          '/usr/bin/kafka-broker-api-versions',
                          '--bootstrap-server',
                          'PLAINTEXT://localhost:9092'),
                         stdout=subprocess.PIPE)
    p.communicate()[0]
    return p.returncode == 0


def kafka_produce(kafka_id, topic, messages):
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


def kafka_check_json_numbers(instance, insert_malformed=False, table='test.kafka', select_count=3):
    retries = 0
    while True:
        if kafka_is_available(instance.cluster.kafka_docker_id):
            break
        else:
            retries += 1
            if retries > 50:
                raise 'Cannot connect to kafka.'
            print("Waiting for kafka to be available...")
            time.sleep(1)

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(instance.cluster.kafka_docker_id, 'json', messages)

    if insert_malformed:
        # Insert couple of malformed messages.
        kafka_produce(instance.cluster.kafka_docker_id, 'json', '}{very_broken_message,\n')
        kafka_produce(instance.cluster.kafka_docker_id, 'json', '}{very_broken_message,\n')

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(instance.cluster.kafka_docker_id, 'json', messages)

    # XXX: since the broken message breaks the `select` reading
    #      we'll try to select a limited number of times.
    result = ''
    for i in range(select_count):
        time.sleep(1)
        result += instance.query('SELECT * FROM {};'.format(table))

    fpath = p.join(p.dirname(__file__), 'test_kafka_json.reference')
    with open(fpath) as reference:
        assert TSV(result) == TSV(reference)


def test_kafka_json(started_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('kafka1:9092', 'json', 'json',
                           'JSONEachRow', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    kafka_check_json_numbers(instance)

    instance.query('DROP TABLE test.kafka')


def test_kafka_json_settings(started_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'json',
                kafka_group_name = 'json',
                kafka_format = 'JSONEachRow',
                kafka_row_delimiter = '\\n',
                kafka_skip_broken_messages = 1;
        ''')

    kafka_check_json_numbers(instance, True)

    instance.query('DROP TABLE test.kafka')


def test_kafka_json_materialized_view(started_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka1:9092',
                kafka_topic_list = 'json',
                kafka_group_name = 'json',
                kafka_format = 'JSONEachRow',
                kafka_row_delimiter = '\\n',
                kafka_skip_broken_messages = 2;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    kafka_check_json_numbers(instance, True, 'test.view', 1)

    instance.query('''
        DROP TABLE test.kafka;
        DROP TABLE test.view;
        DROP TABLE test.consumer;
    ''')


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
