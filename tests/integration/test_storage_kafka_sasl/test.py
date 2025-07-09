import logging
import pytest
import time

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.config_cluster import kafka_sasl_user, kafka_sasl_pass
from helpers.test_tools import assert_eq_with_retry

from kafka import KafkaProducer

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    with_kafka_sasl=True
)

def get_kafka_producer(port):
    errors = []
    for _ in range(15):
        try:
            producer = KafkaProducer(
                bootstrap_servers = "localhost:{}".format(port),
                security_protocol = "SASL_PLAINTEXT",
                sasl_mechanism    = "PLAIN",
                sasl_plain_username = kafka_sasl_user,
                sasl_plain_password = kafka_sasl_pass,
                value_serializer = lambda v: v.encode('utf-8'),
            )
            logging.debug("Kafka Connection established: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not established, {}".format(errors))

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_sasl_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    yield

def test_kafka_sasl(kafka_cluster):
    instance.query(
        f"""
        DROP DATABASE IF EXISTS test SYNC;
        CREATE DATABASE test;

        CREATE TABLE test.kafka_sasl (key int, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka_sasl:19092',
                     kafka_security_protocol = 'sasl_plaintext',
                     kafka_sasl_mechanism = 'PLAIN',
                     kafka_sasl_username = '{kafka_sasl_user}',
                     kafka_sasl_password = '{kafka_sasl_pass}',
                     kafka_topic_list = 'topic1',
                     kafka_group_name = 'group1',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.messages (key int, value String) ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.messages (key int, value String)
        AS SELECT key, value FROM test.kafka_sasl;
        """
    )
    
    producer = get_kafka_producer(kafka_cluster.kafka_sasl_port)
    producer.send(topic="topic1", value='{"key":1, "value":"test123"}')
    producer.flush()

    assert_eq_with_retry(instance, "SELECT value FROM test.messages", "test123")
