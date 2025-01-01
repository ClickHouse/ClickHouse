import json
import logging
import os.path as p
import random
import socket
import subprocess
import threading
import time

import kafka.errors
import pytest
from kafka import BrokerConnection, KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsRequest_v1, DescribeGroupsResponse_v1
from kafka.protocol.group import MemberAssignment

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, is_arm
from helpers.network import PartitionManager
from helpers.test_tools import TSV

if is_arm():
    # skip due to no arm support for clickhouse/kerberos-kdc docker image
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=["configs/users.xml"],
    with_kerberized_kafka=True,
    clickhouse_path_dir="clickhouse_path",
)


def producer_serializer(x):
    return x.encode() if isinstance(x, str) else x


def get_kafka_producer(port, serializer):
    errors = []
    for _ in range(15):
        try:
            producer = KafkaProducer(
                bootstrap_servers="localhost:{}".format(port),
                value_serializer=serializer,
            )
            logging.debug("Kafka Connection establised: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not establised, {}".format(errors))


def kafka_produce(kafka_cluster, topic, messages, timestamp=None):
    logging.debug(
        "kafka_produce server:{}:{} topic:{}".format(
            "localhost", kafka_cluster.kerberized_kafka_port, topic
        )
    )
    producer = get_kafka_producer(
        kafka_cluster.kerberized_kafka_port, producer_serializer
    )
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()


# Fixtures


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        if instance.is_debug_build():
            # https://github.com/ClickHouse/ClickHouse/issues/27651
            pytest.skip(
                "librdkafka calls system function for kinit which does not pass harmful check in debug build"
            )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    yield  # run test


# Tests


def test_kafka_json_as_string(kafka_cluster):
    kafka_produce(
        kafka_cluster,
        "kafka_json_as_string",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            "",
            '{"t": 124, "e": {"x": "test"} }',
            '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}',
        ],
    )

    instance.query(
        """
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string',
                     kafka_commit_on_select = 1,
                     kafka_group_name = 'kafka_json_as_string',
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        """
    )

    time.sleep(3)

    result = instance.query("SELECT * FROM test.kafka;")
    expected = """\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
"""
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log(
        "Parsing of message (topic: kafka_json_as_string, partition: 0, offset: 1) return no rows"
    )


def test_kafka_json_as_string_request_new_ticket_after_expiration(kafka_cluster):
    # Ticket should be expired after the wait time
    # On run of SELECT query new ticket should be requested and SELECT query should run fine.

    kafka_produce(
        kafka_cluster,
        "kafka_json_as_string_after_expiration",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            "",
            '{"t": 124, "e": {"x": "test"} }',
            '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}',
        ],
    )

    instance.query(
        """
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string_after_expiration',
                     kafka_commit_on_select = 1,
                     kafka_group_name = 'kafka_json_as_string_after_expiration',
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        """
    )

    time.sleep(45)  # wait for ticket expiration

    result = instance.query("SELECT * FROM test.kafka;")
    expected = """\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
"""
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log(
        "Parsing of message (topic: kafka_json_as_string_after_expiration, partition: 0, offset: 1) return no rows"
    )


def test_kafka_json_as_string_no_kdc(kafka_cluster):
    # When the test is run alone (not preceded by any other kerberized kafka test),
    # we need a ticket to
    # assert instance.contains_in_log("Ticket expired")
    instance.query(
        """
        CREATE TABLE test.kafka_no_kdc_warm_up (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string_no_kdc_warm_up',
                     kafka_group_name = 'kafka_json_as_string_no_kdc_warm_up',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        """
    )

    instance.query("SELECT * FROM test.kafka_no_kdc_warm_up;")

    kafka_produce(
        kafka_cluster,
        "kafka_json_as_string_no_kdc",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            "",
            '{"t": 124, "e": {"x": "test"} }',
            '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}',
        ],
    )

    # temporary prevent CH - KDC communications
    with PartitionManager() as pm:
        other_node = "kafka_kerberos"
        for node in kafka_cluster.instances.values():
            source = node.ip_address
            destination = kafka_cluster.get_instance_ip(other_node)
            logging.debug(f"partitioning source {source}, destination {destination}")
            pm._add_rule(
                {
                    "source": source,
                    "destination": destination,
                    "action": "REJECT",
                    "protocol": "all",
                }
            )

        time.sleep(45)  # wait for ticket expiration

        instance.query(
            """
            CREATE TABLE test.kafka_no_kdc (field String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kerberized_kafka1:19092',
                         kafka_topic_list = 'kafka_json_as_string_no_kdc',
                         kafka_group_name = 'kafka_json_as_string_no_kdc',
                         kafka_commit_on_select = 1,
                         kafka_format = 'JSONAsString',
                         kafka_flush_interval_ms=1000;
            """
        )

        result = instance.query("SELECT * FROM test.kafka_no_kdc;")
    expected = ""

    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log("StorageKafka (kafka_no_kdc): Nothing to commit")
    assert instance.contains_in_log("Ticket expired")
    assert instance.contains_in_log("KerberosInit failure:")


def test_kafka_config_from_sql_named_collection(kafka_cluster):
    kafka_produce(
        kafka_cluster,
        "kafka_json_as_string_named_collection",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            "",
            '{"t": 124, "e": {"x": "test"} }',
            '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}',
        ],
    )

    instance.query(
        """
        DROP NAMED COLLECTION IF EXISTS kafka_config;
        CREATE NAMED COLLECTION kafka_config AS
            kafka.security_protocol = 'SASL_PLAINTEXT',
            kafka.sasl_mechanism = 'GSSAPI',
            kafka.sasl_kerberos_service_name = 'kafka',
            kafka.sasl_kerberos_keytab = '/tmp/keytab/clickhouse.keytab',
            kafka.sasl_kerberos_principal = 'anotherkafkauser/instance@TEST.CLICKHOUSE.TECH',
            kafka.debug = 'security',
            kafka.api_version_request = 'false',

            kafka_broker_list = 'kerberized_kafka1:19092',
            kafka_topic_list = 'kafka_json_as_string_named_collection',
            kafka_commit_on_select = 1,
            kafka_group_name = 'kafka_json_as_string_named_collection',
            kafka_format = 'JSONAsString',
            kafka_flush_interval_ms=1000;
        """
    )
    instance.query(
        """
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka(kafka_config);
        """
    )

    time.sleep(3)

    result = instance.query("SELECT * FROM test.kafka;")
    expected = """\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
"""
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log(
        "Parsing of message (topic: kafka_json_as_string_named_collection, partition: 0, offset: 1) return no rows"
    )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
