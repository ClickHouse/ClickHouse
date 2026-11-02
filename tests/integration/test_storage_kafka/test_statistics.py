import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import helpers.kafka.common as k


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_no_statistics.xml", "configs/named_collection.xml"],
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


def get_stat_ts(kafka_table):
    result = instance.query(
        f"SELECT JSONExtractInt(rdkafka_stat, 'ts') FROM system.kafka_consumers "
        f"WHERE table = '{kafka_table}' AND rdkafka_stat != '' LIMIT 1 FORMAT TSV"
    ).strip()
    return int(result) if result else None


def wait_for_statistics(kafka_table, timeout=45):
    """Waits until rdkafka_stat is populated and its timestamp advances at least once."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ts1 = get_stat_ts(kafka_table)
        if ts1 is not None:
            for _ in range(int(timeout)):
                ts2 = get_stat_ts(kafka_table)
                if ts2 is not None and ts2 > ts1:
                    return
                time.sleep(1)
            pytest.fail(
                f"rdkafka_stat for '{kafka_table}' is stale (ts {ts1} -> {get_stat_ts(kafka_table)}): "
                "librdkafka statistics events are not being served"
            )
        time.sleep(1)
    pytest.fail(f"rdkafka_stat for '{kafka_table}' is empty: statistics are not enabled")


# Fixtures
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
    yield  # run test


def test_statistics_enabled_by_default(kafka_cluster):
    """rdkafka_stat must be populated without any explicit statistics config (pre-seeded default).

    Covers both engines: the default for StorageKafka was dead code (has_property() is always true
    for a known librdkafka property), and StorageKafka2 used to override even an explicit value."""
    topic_name = "topic_statistics_default"

    old_engine_table = k.generate_old_create_table_query(
        "kafka_old", "a String", topic_list=topic_name, format="LineAsString"
    )
    new_engine_table = k.generate_new_create_table_query(
        "kafka_new", "a String", topic_list=topic_name, format="LineAsString"
    )

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dst_old (a String) ENGINE = MergeTree() ORDER BY tuple();
            CREATE TABLE test.dst_new (a String) ENGINE = MergeTree() ORDER BY tuple();
            {old_engine_table};
            {new_engine_table};
            CREATE MATERIALIZED VIEW test.kafka_old_mv TO test.dst_old AS SELECT a FROM test.kafka_old;
            CREATE MATERIALIZED VIEW test.kafka_new_mv TO test.dst_new AS SELECT a FROM test.kafka_new;
            """
        )

        k.kafka_produce(kafka_cluster, topic_name, ["message_1"])
        instance.query_with_retry(
            "SELECT * FROM test.dst_old",
            retry_count=60,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 1,
        )
        instance.query_with_retry(
            "SELECT * FROM test.dst_new",
            retry_count=60,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 1,
        )

        # The default interval is 3000ms, so waiting for a ts advance takes a few seconds.
        wait_for_statistics("kafka_old")
        wait_for_statistics("kafka_new")


def test_statistics_survive_mv_detach(kafka_cluster):
    """StorageKafka2 must keep serving librdkafka events after the materialized view is detached.

    Without the drain, statistics events accumulate in the librdkafka reply queue indefinitely
    (a live memory leak) while nobody polls the idle consumer."""
    topic_name = "topic_statistics_detach"

    create_kafka_query = k.generate_new_create_table_query(
        "kafka", "a String", topic_list=topic_name, format="LineAsString"
    )

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dst (a String) ENGINE = MergeTree() ORDER BY tuple();
            {create_kafka_query};
            CREATE MATERIALIZED VIEW test.kafka_mv TO test.dst AS SELECT a FROM test.kafka;
            """
        )

        k.kafka_produce(kafka_cluster, topic_name, ["message_1"])
        instance.query_with_retry(
            "SELECT * FROM test.dst",
            retry_count=60,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 1,
        )
        wait_for_statistics("kafka")

        # Detach the view: the consumer becomes idle, but events must still be served.
        instance.query("DROP TABLE test.kafka_mv SYNC")
        wait_for_statistics("kafka")

        # Re-attaching the view must continue consuming without losses.
        instance.query("CREATE MATERIALIZED VIEW test.kafka_mv TO test.dst AS SELECT a FROM test.kafka")
        k.kafka_produce(kafka_cluster, topic_name, ["message_2"])
        instance.query_with_retry(
            "SELECT * FROM test.dst",
            retry_count=60,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 2,
        )
        assert TSV(instance.query("SELECT * FROM test.dst ORDER BY a")) == TSV("message_1\nmessage_2")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
