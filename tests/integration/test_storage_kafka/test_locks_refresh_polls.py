import json

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
        instance.wait_for_log_line(rf"Changed {SETTING} from 15 to 3")

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
        instance.wait_for_log_line(rf"Changed {SETTING} from 3 to 15")


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
