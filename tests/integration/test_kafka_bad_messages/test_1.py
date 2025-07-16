from helpers.kafka_common import (
    kafka_create_topic,
    kafka_delete_topic,
    get_kafka_producer,
    producer_serializer,
    kafka_produce,
)

import logging
import time

import pytest
from kafka import BrokerConnection, KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.kafka_common import (
    kafka_create_topic,
    kafka_delete_topic,
    get_kafka_producer,
    producer_serializer,
    kafka_produce,
)

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    with_kafka=True,
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()


def test_system_kafka_consumers_grant(kafka_cluster, max_retries=20):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    kafka_create_topic(admin_client, "visible")
    kafka_create_topic(admin_client, "hidden")
    instance.query(
        f"""
        DROP TABLE IF EXISTS kafka_grant_visible;
        DROP TABLE IF EXISTS kafka_grant_hidden;

        CREATE TABLE kafka_grant_visible (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'visible',
                     kafka_group_name = 'visible',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms=1000,
                     kafka_num_consumers = 1;

        CREATE TABLE kafka_grant_hidden (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'hidden',
                     kafka_group_name = 'hidden',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms=1000,
                     kafka_num_consumers = 1;
    """
    )

    result_system_kafka_consumers = instance.query_with_retry(
        """
        SELECT count(1) FROM system.kafka_consumers WHERE table LIKE 'kafka_grant%'
        """,
        retry_count=max_retries,
        sleep_time=1,
        check_callback=lambda res: int(res) == 2,
    )
    # both kafka_grant_hidden and kafka_grant_visible tables are visible

    instance.query(
        f"""
        DROP USER IF EXISTS restricted;
        CREATE USER restricted;
        GRANT SHOW ON default.kafka_grant_visible TO restricted;
        GRANT SELECT ON system.kafka_consumers TO restricted;
    """
    )

    restricted_result_system_kafka_consumers = instance.query(
        "SELECT count(1) FROM system.kafka_consumers WHERE table LIKE 'kafka_grant%'",
        user="restricted",
    )
    assert int(restricted_result_system_kafka_consumers) == 1
    # only kafka_grant_visible is visible for user `restricted`

    kafka_delete_topic(admin_client, "visible")
    kafka_delete_topic(admin_client, "hidden")
    instance.query(
        f"""
        DROP TABLE IF EXISTS kafka_grant_visible;
        DROP TABLE IF EXISTS kafka_grant_hidden;
        DROP USER IF EXISTS restricted;
    """
    )


def test_log_to_exceptions(kafka_cluster, max_retries=20):

    non_existent_broker_port = 9876
    instance.query(
        f"""
        DROP TABLE IF EXISTS foo_exceptions;

        CREATE TABLE foo_exceptions(a String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'localhost:{non_existent_broker_port}', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'RawBLOB';
    """
    )

    instance.query(
        "SELECT * FROM foo_exceptions SETTINGS stream_like_engine_allow_direct_select=1"
    )
    instance.query("SYSTEM FLUSH LOGS")

    system_kafka_consumers_content = instance.query(
        "SELECT exceptions.text FROM system.kafka_consumers ARRAY JOIN exceptions WHERE table LIKE 'foo_exceptions' LIMIT 1"
    )

    logging.debug(f"system.kafka_consumers content: {system_kafka_consumers_content}")
    assert system_kafka_consumers_content.startswith(
        f"[thrd:localhost:{non_existent_broker_port}/bootstrap]: localhost:{non_existent_broker_port}/bootstrap: Connect to ipv4#127.0.0.1:{non_existent_broker_port} failed: Connection refused"
    )

    instance.query("DROP TABLE foo_exceptions")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
