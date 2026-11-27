import json
import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_and_keeper.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "locks_refresh_topic",
        "kafka_group_name_new": "locks_refresh_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
)

SETTING = "stream_kafka_locks_refresh_polls"
OVERRIDE_PATH = "/etc/clickhouse-server/config.d/zz_kafka_locks_refresh_polls.xml"


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield
    set_locks_refresh_polls(None)


def set_locks_refresh_polls(value):
    """Write (or remove, for None) a config.d override and reload the config."""
    if value is None:
        instance.exec_in_container(["bash", "-c", f"rm -f {OVERRIDE_PATH}"])
    else:
        instance.replace_config(
            OVERRIDE_PATH,
            f"<clickhouse><{SETTING}>{value}</{SETTING}></clickhouse>",
        )
    instance.query("SYSTEM RELOAD CONFIG")


def current_value():
    return int(
        instance.query(
            f"SELECT value FROM system.server_settings WHERE name = '{SETTING}'"
        ).strip()
    )


def create_kafka2_table(topic_name, suffix):
    instance.query(
        f"""
        CREATE TABLE test.kafka_{suffix} (key UInt64, value UInt64)
        ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg', 'JSONEachRow', '\\n')
        SETTINGS kafka_keeper_path = '/clickhouse/test/{suffix}',
                 kafka_replica_name = 'r1',
                 kafka_flush_interval_ms = 500
        SETTINGS allow_kafka_offsets_storage_in_keeper = 1;

        CREATE TABLE test.dst_{suffix} (key UInt64, value UInt64)
        ENGINE = MergeTree() ORDER BY key;

        CREATE MATERIALIZED VIEW test.mv_{suffix} TO test.dst_{suffix} AS
        SELECT * FROM test.kafka_{suffix};
        """
    )


def produce(kafka_cluster, topic_name, start, count):
    messages = [json.dumps({"key": i, "value": i}) for i in range(start, start + count)]
    k.kafka_produce(kafka_cluster, topic_name, messages)


def wait_for_count(table, expected, timeout=60):
    instance.query_with_retry(
        f"SELECT count() FROM {table}",
        check_callback=lambda res: int(res.strip()) >= expected,
        retry_count=timeout,
        sleep_time=1,
    )
    assert int(instance.query(f"SELECT count() FROM {table}").strip()) == expected


def test_locks_refresh_polls_setting_metadata(kafka_cluster):
    assert current_value() == 15
    assert (
        instance.query(
            f"SELECT changeable_without_restart FROM system.server_settings WHERE name = '{SETTING}'"
        ).strip()
        == "Yes"
    )


def test_locks_refresh_polls_reload(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "locks_refresh_reload"
    suffix = "locks_refresh_reload"

    with k.kafka_topic(admin, topic_name, num_partitions=2):
        create_kafka2_table(topic_name, suffix)
        produce(kafka_cluster, topic_name, 0, 10)
        wait_for_count(f"test.dst_{suffix}", 10)

        # The default is used until the config says otherwise. A consumer refreshes
        # once poll_count exceeds the threshold, so the logged count is threshold + 1.
        instance.wait_for_log_line(
            rf"refreshing its Keeper locks after 16 polls \({SETTING} = 15\)",
            timeout=120,
        )

        set_locks_refresh_polls(3)
        assert current_value() == 3

        # Running consumers pick the new value up without recreating the table.
        instance.wait_for_log_line(
            rf"refreshing its Keeper locks after 4 polls \({SETTING} = 3\)",
            timeout=120,
        )
        produce(kafka_cluster, topic_name, 10, 10)
        wait_for_count(f"test.dst_{suffix}", 20)

        # Removing the override restores the default on the next reload.
        set_locks_refresh_polls(None)
        assert current_value() == 15


def test_locks_refresh_polls_zero_still_consumes(kafka_cluster):
    """0 refreshes on every poll cycle: slow, but no message may be lost or duplicated."""
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "locks_refresh_zero"
    suffix = "locks_refresh_zero"

    set_locks_refresh_polls(0)
    assert current_value() == 0

    with k.kafka_topic(admin, topic_name, num_partitions=2):
        create_kafka2_table(topic_name, suffix)
        produce(kafka_cluster, topic_name, 0, 50)
        wait_for_count(f"test.dst_{suffix}", 50, timeout=120)
        assert (
            int(instance.query(f"SELECT uniqExact(key) FROM test.dst_{suffix}").strip())
            == 50
        )


def test_kafka2_partition_of_recreated_topic(kafka_cluster):
    """A partition re-assigned while its topic is deleted is read after the topic is recreated."""
    admin = k.get_admin_client(kafka_cluster)
    # Unique per run, so that `wait_for_deletion` cannot match the log line of a previous run.
    suffix = f"recreated_topic_{k.random_string(6)}"
    topic_a = f"{suffix}_a"
    topic_b = f"{suffix}_b"

    # Only the rollbacks of the direct SELECTs below re-initialize the consumer.
    set_locks_refresh_polls(10000)

    def select_keys():
        return instance.query(f"SELECT key FROM test.kafka_{suffix}").split()

    def wait_for_key(key, timeout=30):
        deadline = time.monotonic() + timeout
        while str(key) not in select_keys():
            assert time.monotonic() < deadline, f"key {key} was not read"
            time.sleep(0.5)

    def wait_for_deletion(topic):
        instance.wait_for_log_line(
            rf"Topic {topic} \(id [^)]*\) partition count changed from 1 to 0",
            timeout=60,
        )

    k.kafka_create_topic(admin, topic_a)
    k.kafka_create_topic(admin, topic_b)
    try:
        instance.query(
            f"""
            CREATE TABLE test.kafka_{suffix} (key UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_a},{topic_b}', '{suffix}_cg', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/{suffix}',
                     kafka_replica_name = 'r1',
                     kafka_flush_interval_ms = 500,
                     kafka_consumers_pool_ttl_ms = 600000
            SETTINGS allow_kafka_offsets_storage_in_keeper = 1
            """
        )
        produce(kafka_cluster, topic_a, 1, 1)
        wait_for_key(1)

        k.kafka_delete_topic(admin, topic_b)
        wait_for_deletion(topic_b)
        # Reading key 1 again rolls back, which re-assigns B:0 while librdkafka does not know it.
        wait_for_key(1)

        k.kafka_delete_topic(admin, topic_a)
        wait_for_deletion(topic_a)
        # Drain a prefetched copy of key 1, so that no later SELECT rolls back.
        deadline = time.monotonic() + 30
        empty_selects = 0
        while empty_selects < 2:
            assert time.monotonic() < deadline, "SELECTs keep returning rows"
            empty_selects = 0 if select_keys() else empty_selects + 1

        k.kafka_create_topic(admin, topic_b)
        produce(kafka_cluster, topic_b, 3, 1)
        wait_for_key(3, timeout=60)
    finally:
        instance.query(f"DROP TABLE IF EXISTS test.kafka_{suffix} SYNC")
        for topic in (topic_a, topic_b):
            if topic in admin.list_topics():
                k.kafka_delete_topic(admin, topic)
