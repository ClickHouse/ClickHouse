import os.path as p
import time
import datetime
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

from kafka import KafkaProducer
import json



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

def test_kafka_json(started_cluster):
    instance.query('''
DROP TABLE IF EXISTS test.kafka;
CREATE TABLE test.kafka (key UInt64, value UInt64)
    ENGINE = Kafka('kafka1:9092', 'json', 'json', 'JSONEachRow', '\\n');
''')

    retries = 0
    while True:
        try:
            producer = KafkaProducer()
            break
        except:
            retries += 1
            if retries > 50:
                raise
            print("Waiting for kafka to be available...")
            time.sleep(1)
    for i in xrange(50):
        producer.send('json', json.dumps({'key': i, 'value': i}))
    producer.flush()
    time.sleep(3)
    result = instance.query('SELECT * FROM test.kafka;')
    with open(p.join(p.dirname(__file__), 'test_kafka_json.reference')) as reference:
        assert TSV(result) == TSV(reference)
    instance.query('DROP TABLE test.kafka')
