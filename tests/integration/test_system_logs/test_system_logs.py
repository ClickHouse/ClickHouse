# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    base_config_dir="configs",
    main_configs=["configs/config.d/system_logs_order_by.xml"],
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    base_config_dir="configs",
    main_configs=[
        "configs/config.d/system_logs_engine.xml",
        "configs/config.d/disks.xml",
    ],
    stay_alive=True,
)

node3 = cluster.add_instance(
    "node3",
    base_config_dir="configs",
    main_configs=[
        "configs/config.d/system_logs_settings.xml",
        "configs/config.d/disks.xml",
    ],
    stay_alive=True,
)


node4 = cluster.add_instance(
    "node4",
    base_config_dir="configs",
    main_configs=[
        "configs/config.d/system_logs_engine_s3_plain_rewritable_policy.xml",
        "configs/config.d/disks.xml",
    ],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_logs_order_by_expr(start_cluster):
    node1.query("SET log_query_threads = 1")
    node1.query("SELECT count() FROM system.tables")
    node1.query("SYSTEM FLUSH LOGS")

    # Check 'sorting_key' of system.query_log.
    assert (
        node1.query(
            "SELECT sorting_key FROM system.tables WHERE database='system' and name='query_log'"
        )
        == "event_date, event_time, initial_query_id\n"
    )

    # Check 'sorting_key' of  system.query_thread_log.
    assert (
        node1.query(
            "SELECT sorting_key FROM system.tables WHERE database='system' and name='query_thread_log'"
        )
        == "event_date, event_time, query_id\n"
    )


def test_system_logs_engine_expr(start_cluster):
    node2.query("SET log_query_threads = 1")
    node2.query("SELECT count() FROM system.tables")
    node2.query("SYSTEM FLUSH LOGS")

    # Check 'engine_full' of system.query_log.
    expected = "MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + toIntervalDay(30) SETTINGS storage_policy = \\'policy2\\', ttl_only_drop_parts = 1"
    assert expected in node2.query(
        "SELECT engine_full FROM system.tables WHERE database='system' and name='query_log'"
    )


def test_system_logs_engine_s3_plain_rw_expr(start_cluster):
    node4.query("SET log_query_threads = 1")
    node4.query("SELECT count() FROM system.tables")
    node4.query("SYSTEM FLUSH LOGS")

    # Check 'engine_full' of system.query_log.
    expected = "MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + toIntervalDay(30) SETTINGS storage_policy = \\'s3_plain_rewritable\\', ttl_only_drop_parts = 1"
    assert expected in node4.query(
        "SELECT engine_full FROM system.tables WHERE database='system' and name='query_log'"
    )
    node4.restart_clickhouse()
    assert expected in node4.query(
        "SELECT engine_full FROM system.tables WHERE database='system' and name='query_log'"
    )


def test_system_logs_settings_expr(start_cluster):
    node3.query("SET log_query_threads = 1")
    node3.query("SELECT count() FROM system.tables")
    node3.query("SYSTEM FLUSH LOGS")

    # Check 'engine_full' of system.query_log.
    expected = "MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time, initial_query_id) TTL event_date + toIntervalDay(30) SETTINGS storage_policy = \\'policy1\\', storage_policy = \\'policy2\\', ttl_only_drop_parts = 1"
    assert expected in node3.query(
        "SELECT engine_full FROM system.tables WHERE database='system' and name='query_log'"
    )


def test_max_size_0(start_cluster):
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <max_size_rows replace=\\"replace\\">0</max_size_rows> 
                <reserved_size_rows replace=\\"replace\\">0</reserved_size_rows>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    with pytest.raises(Exception):
        node1.restart_clickhouse()

    node1.exec_in_container(
        ["rm", f"/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )
    node1.restart_clickhouse()


def test_reserved_size_greater_max_size(start_cluster):
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <max_size_rows replace=\\"replace\\">10</max_size_rows>
                <reserved_size_rows replace=\\"replace\\">11</reserved_size_rows> 
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    with pytest.raises(Exception):
        node1.restart_clickhouse()

    node1.exec_in_container(
        ["rm", f"/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )
    node1.restart_clickhouse()
