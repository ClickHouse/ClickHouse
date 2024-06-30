# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_default",
    stay_alive=True,
)

system_logs = [
    # disabled by default
    ("system.text_log", 0),
    # enabled by default
    ("system.query_log", 1),
    ("system.query_thread_log", 1),
    ("system.part_log", 1),
    ("system.trace_log", 1),
    ("system.metric_log", 1),
    ("system.error_log", 1),
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def flush_logs():
    node.query("SYSTEM FLUSH LOGS")


@pytest.mark.parametrize("table,exists", system_logs)
def test_system_logs(flush_logs, table, exists):
    q = "SELECT * FROM {}".format(table)
    if exists:
        node.query(q)
    else:
        response = node.query_and_get_error(q)
        assert (
            "Table {} does not exist".format(table) in response
            or "Unknown table expression identifier '{}'".format(table) in response
        )


# Logic is tricky, let's check that there is no hang in case of message queue
# is not empty (this is another code path in the code).
def test_system_logs_non_empty_queue():
    node.query(
        "SELECT 1",
        settings={
            # right now defaults are the same,
            # this set explicitly to avoid depends from defaults.
            "log_queries": 1,
            "log_queries_min_type": "QUERY_START",
        },
    )
    node.query("SYSTEM FLUSH LOGS")


def test_system_suspend():
    node.query("CREATE TABLE t (x DateTime) ENGINE=Memory;")
    node.query("INSERT INTO t VALUES (now());")
    node.query("SYSTEM SUSPEND FOR 1 SECOND;")
    node.query("INSERT INTO t VALUES (now());")
    assert "1\n" == node.query("SELECT max(x) - min(x) >= 1 FROM t;")


def test_log_max_size(start_cluster):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <flush_interval_milliseconds replace=\\"replace\\">1000000</flush_interval_milliseconds>
                <max_size_rows replace=\\"replace\\">10</max_size_rows>
                <reserved_size_rows replace=\\"replace\\">10</reserved_size_rows>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    node.restart_clickhouse()
    for i in range(10):
        node.query(f"select {i}")

    assert node.query("select count() >= 10 from system.query_log") == "1\n"
    node.exec_in_container(
        ["rm", f"/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )


def test_log_buffer_size_rows_flush_threshold(start_cluster):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <flush_interval_milliseconds replace=\\"replace\\">1000000</flush_interval_milliseconds>
                <buffer_size_rows_flush_threshold replace=\\"replace\\">10</buffer_size_rows_flush_threshold>
                <max_size_rows replace=\\"replace\\">10000</max_size_rows>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    node.restart_clickhouse()
    node.query(f"TRUNCATE TABLE IF EXISTS system.query_log")
    for i in range(10):
        node.query(f"select {i}")

    assert_eq_with_retry(
        node,
        f"select count() >= 11 from system.query_log",
        "1",
        sleep_time=0.2,
        retry_count=100,
    )

    node.query(f"TRUNCATE TABLE IF EXISTS system.query_log")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <flush_interval_milliseconds replace=\\"replace\\">1000000</flush_interval_milliseconds>
                <buffer_size_rows_flush_threshold replace=\\"replace\\">10000</buffer_size_rows_flush_threshold>
                <max_size_rows replace=\\"replace\\">10000</max_size_rows>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    node.restart_clickhouse()
    for i in range(10):
        node.query(f"select {i}")

    # Logs aren't flushed
    assert_eq_with_retry(
        node,
        f"select count() < 10 from system.query_log",
        "1",
        sleep_time=0.2,
        retry_count=100,
    )

    node.exec_in_container(
        ["rm", f"/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )
