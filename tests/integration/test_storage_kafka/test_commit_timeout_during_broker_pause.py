"""Pins that StorageKafkaUtils::COMMIT_TIMEOUT_MS is what returns an offset commit from a broker
that stopped answering, for both consumer implementations. test_batch_slow_6 already covers commit
failure recovery; what is pinned here is the bound itself — the failure line lands ~30s after the
broker freezes, ahead of librdkafka's own 60s request timeout and of the stall the deadline replaces.
"""

import json
import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
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
        "kafka_topic_old": k.KAFKA_TOPIC_OLD,
        "kafka_group_name_old": k.KAFKA_CONSUMER_GROUP_OLD,
        "kafka_topic_new": k.KAFKA_TOPIC_NEW,
        "kafka_group_name_new": k.KAFKA_CONSUMER_GROUP_NEW,
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
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


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield


@pytest.mark.parametrize(
    "create_query_generator, commit_failure",
    [
        (k.generate_old_create_table_query, "Exception during commit attempt"),
        (k.generate_new_create_table_query, "Exception during attempt to commit to Kafka"),
    ],
)
def test_kafka_commit_deadline_returns_during_broker_pause(
    kafka_cluster, create_query_generator, commit_failure
):
    suffix = k.random_string(6)
    table = f"kafka_{suffix}"
    topic_name = f"commit_deadline_{k.get_topic_postfix(create_query_generator)}_{suffix}"

    # The MV's sleepEachRow holds it inside the first 20-row block for 5s, so the pause lands
    # between the poll and the commit — the same window shape as
    # test_batch_slow_6::test_kafka_handling_commit_failure.
    messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    create_query = create_query_generator(
        table,
        "key UInt64, value String",
        topic_list=topic_name,
        consumer_group=topic_name,
        format="JSONEachRow",
        settings={"kafka_max_block_size": 20, "kafka_flush_interval_ms": 1000},
    )
    instance.query(f"""
        DROP TABLE IF EXISTS test.{table}_mv SYNC;
        DROP TABLE IF EXISTS test.{table}_view SYNC;
        DROP TABLE IF EXISTS test.{table} SYNC;

        {create_query};

        CREATE TABLE test.{table}_view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_view AS
            SELECT * FROM test.{table}
            WHERE NOT sleepEachRow(0.25);
    """)

    instance.wait_for_log_line(f"{table}.*Polled batch of 20 messages", timeout=60)

    start_time = time.monotonic()
    with kafka_cluster.pause_container("kafka1"):
        # wait_for_log_line raises once 48s pass without the line, so it is the upper bound:
        # past the 30s+window deadline firing, and short of the 60s at which librdkafka's own
        # request timeout would surface on an unbounded commit.
        instance.wait_for_log_line(f"{table}.*{commit_failure}", timeout=48)
        elapsed = time.monotonic() - start_time

    # The first commit after the pause is issued at most one flush interval later, so a line
    # before ~28s means something answered earlier than the deadline — not the deadline returning.
    assert elapsed >= 28, elapsed

    # The unpause frees the retry, the task commits the rest of the backlog on its own —
    # the only exit from the stall this deadline replaces was a server restart.
    instance.wait_for_log_line(f"{table}.*Committed offset 22", timeout=120)
    uniq_and_max = instance.query(f"SELECT uniqExact(key), max(key) FROM test.{table}_view")
    count = int(instance.query(f"SELECT count() FROM test.{table}_view"))
    logging.debug(f"{table}: {uniq_and_max.strip()} over {count} rows, deadline line at {elapsed:.1f}s")
    assert TSV(uniq_and_max) == TSV("22\t22")
    assert count >= 22

    instance.query(f"""
        DROP TABLE test.{table}_mv SYNC;
        DROP TABLE test.{table}_view SYNC;
        DROP TABLE test.{table} SYNC;
    """)
