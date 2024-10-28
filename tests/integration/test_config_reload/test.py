import pytest

from helpers.cluster import ClickHouseCluster, is_arm

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    with_kafka=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


DEFAULT_VALUE = "424242"
CHANGED_VALUE = "414141"


def check_value(value):
    instance.query(
        f"""
        CREATE TABLE test (x Int64) ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '{cluster.kafka_host}:{cluster.kafka_port}',
            kafka_topic_list = 'config_test',
            kafka_group_name = 'config_test_group',
            kafka_format = 'JSON';
        """
    )

    instance.query(
        "SELECT * FROM test SETTINGS stream_like_engine_allow_direct_select=1",
        ignore_error=True,
    )

    assert instance.wait_for_log_line("Consumer set property session.timeout.ms")
    instance.query("DROP TABLE test SYNC")

    instance.contains_in_log(f"Consumer set property session.timeout.ms:{value}")


def test_system_reload_config_with_global_context(start_cluster):
    # When running the this test multiple times, make sure failure of one test won't cause the failure of every subsequent tests
    instance.query("DROP TABLE IF EXISTS test SYNC")
    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/kafka.xml", CHANGED_VALUE, DEFAULT_VALUE
    )
    instance.restart_clickhouse()

    check_value(DEFAULT_VALUE)

    instance.rotate_logs()

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/kafka.xml", DEFAULT_VALUE, CHANGED_VALUE
    )

    instance.query("SYSTEM RELOAD CONFIG")

    check_value(CHANGED_VALUE)
