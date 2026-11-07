import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV
import helpers.kafka.common as k


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/named_collection.xml", "configs/kafka_short_producer_timeout.xml"],
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
    """rdkafka_stat must be populated without any statistics config for StorageKafka2."""
    topic_name = "topic_statistics_default"

    # Distinct groups: the old engine's broker commits would move the start offset of the Keeper-backed one.
    new_engine_table = k.generate_new_create_table_query(
        "kafka_new", "a String", topic_list=topic_name, consumer_group="statistics_new_engine", format="LineAsString"
    )

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dst_new (a String) ENGINE = MergeTree() ORDER BY tuple();
            {new_engine_table};
            CREATE MATERIALIZED VIEW test.kafka_new_mv TO test.dst_new AS SELECT a FROM test.kafka_new;
            """
        )

        k.kafka_produce(kafka_cluster, topic_name, ["message_1"])
        instance.query_with_retry(
            "SELECT * FROM test.dst_new",
            retry_count=60,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 1,
        )

        # The default interval is 3000ms, so waiting for a ts advance takes a few seconds.
        wait_for_statistics("kafka_new")


def test_statistics_survive_mv_detach(kafka_cluster):
    """StorageKafka2 must keep serving librdkafka events after the materialized view is detached."""
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


def test_statistics_survive_keeper_outage(kafka_cluster):
    """StorageKafka2 must keep serving librdkafka events while its replica is inactive after a Keeper session loss."""
    topic_name = "topic_statistics_keeper_outage"

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

        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(instance)
            instance.wait_for_log_line(
                "ZooKeeper session has expired. Switching to a new session",
                timeout=120,
                look_behind_lines=0,
            )
            wait_for_statistics("kafka")

        k.kafka_produce(kafka_cluster, topic_name, ["message_2"])
        instance.query_with_retry(
            "SELECT * FROM test.dst",
            retry_count=120,
            sleep_time=1,
            check_callback=lambda x: len(TSV(x)) == 2,
        )


def test_statistics_survive_blocked_mv_insert(kafka_cluster):
    """Statistics must keep being served while a blocked MV insert holds the consumer."""
    topic_name = "topic_statistics_stall"

    create_kafka_query = k.generate_new_create_table_query(
        "kafka", "a String", topic_list=topic_name, format="LineAsString"
    )

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dead_dst (a String) ENGINE = Kafka('dead_host:9092', 'dead_dst', 'dead_group', 'LineAsString');
            {create_kafka_query};
            CREATE MATERIALIZED VIEW test.kafka_mv TO test.dead_dst AS SELECT a FROM test.kafka;
            """
        )

        k.kafka_produce(kafka_cluster, topic_name, ["message_1"])

        # Sync on the read completing (the insert starts blocking immediately after).
        deadline_read = time.monotonic() + 10
        while time.monotonic() < deadline_read:
            if instance.query(
                "SELECT count() FROM system.kafka_consumers "
                "WHERE table = 'kafka' AND num_messages_read > 0"
            ).strip() == "1":
                break
            time.sleep(1)

        try:
            ts_first = None
            ts_second = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                ts = get_stat_ts("kafka")
                if ts is not None:
                    if ts_first is None:
                        ts_first = ts
                    elif ts > ts_first:
                        ts_second = ts
                        break
                time.sleep(2)
            assert ts_first is not None and ts_second is not None, (
                f"rdkafka_stat is not served while the MV insert is blocked (ts {ts_first} -> {ts_second}): "
                "statistics events accumulate in the reply queue during a busy-but-not-polling hold"
            )
        finally:
            # The producer timeout (5 s in the test config) bounds the blocked insert,
            # so the teardown only waits that long after dropping the dead destination.
            instance.query("DROP TABLE IF EXISTS test.dead_dst SYNC", ignore_error=True)


def test_statistics_survive_blocked_insert_during_keeper_outage(kafka_cluster):
    """Statistics must keep being served when a blocked MV insert overlaps a Keeper outage."""
    topic_name = "topic_statistics_overlap"

    create_kafka_query = k.generate_new_create_table_query(
        "kafka", "a String", topic_list=topic_name, format="LineAsString"
    )

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dead_dst (a String) ENGINE = Kafka('dead_host:9092', 'dead_dst_overlap', 'dead_group2', 'LineAsString');
            {create_kafka_query};
            CREATE MATERIALIZED VIEW test.kafka_mv TO test.dead_dst AS SELECT a FROM test.kafka;
            """
        )

        k.kafka_produce(kafka_cluster, topic_name, ["message_1"])

        # Sync on the read completing (the insert starts blocking immediately after).
        deadline_read = time.monotonic() + 10
        while time.monotonic() < deadline_read:
            if instance.query(
                "SELECT count() FROM system.kafka_consumers "
                "WHERE table = 'kafka' AND num_messages_read > 0"
            ).strip() == "1":
                break
            time.sleep(1)

        try:
            with PartitionManager() as pm:
                pm.drop_instance_zk_connections(instance)
                instance.wait_for_log_line(
                    "ZooKeeper session has expired. Switching to a new session",
                    timeout=120,
                    look_behind_lines=0,
                )
                # The drain task must keep advancing ts despite the activation task
                # being blocked in partialShutdown() for the remaining insert duration.
                ts_first = None
                ts_second = None
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    ts = get_stat_ts("kafka")
                    if ts is not None:
                        if ts_first is None:
                            ts_first = ts
                        elif ts > ts_first:
                            ts_second = ts
                            break
                    time.sleep(2)
                assert ts_first is not None and ts_second is not None, (
                    f"rdkafka_stat is not served while a blocked insert overlaps a Keeper "
                    f"outage (ts {ts_first} -> {ts_second})"
                )
        finally:
            instance.query("DROP TABLE IF EXISTS test.dead_dst SYNC", ignore_error=True)


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
