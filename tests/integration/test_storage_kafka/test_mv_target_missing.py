import logging
import time

import pytest
from kafka import KafkaAdminClient

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": "old",
        "kafka_group_name_old": "old",
        "kafka_topic_new": "new",
        "kafka_group_name_new": "new",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)
    topics = [t for t in admin_client.list_topics() if not t.startswith("_")]
    if topics:
        admin_client.delete_topics(topics)
    yield


def test_missing_mv_target(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic = "mv_target_missing"
    k.kafka_create_topic(admin, topic)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.target1;
        DROP TABLE IF EXISTS test.target2;
        DROP TABLE IF EXISTS test.mv1;
        DROP TABLE IF EXISTS test.mv2;

        CREATE TABLE test.kafka (a UInt64, b String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\n',
                     format_csv_delimiter = '|';

        CREATE TABLE test.target2 (a UInt64, b String) ENGINE = MergeTree() ORDER BY a;
        CREATE MATERIALIZED VIEW test.mv1 TO test.target1 AS SELECT * FROM test.kafka;
        CREATE MATERIALIZED VIEW test.mv2 TO test.target2 AS SELECT * FROM test.kafka;
        """
    )

    def has_error(res):
        return "Materialized view" in res

    instance.query_with_retry(
        "SELECT exceptions.text[1] FROM system.kafka_consumers WHERE database='test' and table='kafka'",
        check_callback=has_error,
    )

    instance.query("CREATE TABLE test.target1 (a UInt64, b String) ENGINE = MergeTree() ORDER BY a")

    k.kafka_produce(kafka_cluster, topic, ["1|foo", "2|bar"])

    assert instance.query_with_retry(
        "SELECT count() FROM test.target2",
        check_callback=lambda x: int(x) == 2,
    ).strip() == "2"
    assert instance.query_with_retry(
        "SELECT count() FROM test.target1",
        check_callback=lambda x: int(x) == 2,
    ).strip() == "2"

    instance.query(
        """
        DROP TABLE test.mv1;
        DROP TABLE test.mv2;
        DROP TABLE test.target1;
        DROP TABLE test.target2;
        DROP TABLE test.kafka;
        """
    )
    k.kafka_delete_topic(admin, topic)
