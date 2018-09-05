import os.path as p
import time
import datetime
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import json
import subprocess



cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=['configs/kafka.xml'], with_kafka = True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()

def kafka_is_available(started_cluster):
    p = subprocess.Popen(('docker', 'exec', '-i', started_cluster.kafka_docker_id, '/usr/bin/kafka-broker-api-versions', '--bootstrap-server', 'PLAINTEXT://localhost:9092'), stdout=subprocess.PIPE)
    streamdata = p.communicate()[0]
    return p.returncode == 0

def kafka_produce(started_cluster, topic, messages):
    p = subprocess.Popen(('docker', 'exec', '-i', started_cluster.kafka_docker_id, '/usr/bin/kafka-console-producer', '--broker-list', 'localhost:9092', '--topic', topic), stdin=subprocess.PIPE)
    p.communicate(messages)
    p.stdin.close()

def test_kafka_json(started_cluster):
    instance.query('''
DROP TABLE IF EXISTS test.kafka;
CREATE TABLE test.kafka (key UInt64, value UInt64)
    ENGINE = Kafka('kafka1:9092', 'json', 'json', 'JSONEachRow', '\\n');
''')

    retries = 0
    while True:
        if kafka_is_available(started_cluster):
            break
        else:
            retries += 1
            if retries > 50:
                raise 'Cannot connect to kafka.'
            print("Waiting for kafka to be available...")
            time.sleep(1)
    messages = ''
    for i in xrange(50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(started_cluster, 'json', messages)
    time.sleep(3)
    result = instance.query('SELECT * FROM test.kafka;')
    with open(p.join(p.dirname(__file__), 'test_kafka_json.reference')) as reference:
        assert TSV(result) == TSV(reference)
    instance.query('DROP TABLE test.kafka')

if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
