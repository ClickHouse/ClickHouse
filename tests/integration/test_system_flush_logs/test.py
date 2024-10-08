# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_default",
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_logs_exists():
    system_logs = [
        ("system.text_log", 1),
        ("system.query_log", 1),
        ("system.query_thread_log", 1),
        ("system.part_log", 1),
        ("system.trace_log", 1),
        ("system.metric_log", 1),
        ("system.error_log", 1),
    ]

    node.query("SYSTEM FLUSH LOGS")
    for table, exists in system_logs:
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
    try:
        node.query("CREATE TABLE t (x DateTime) ENGINE=Memory;")
        node.query("INSERT INTO t VALUES (now());")
        node.query("SYSTEM SUSPEND FOR 1 SECOND;")
        node.query("INSERT INTO t VALUES (now());")
        assert "1\n" == node.query("SELECT max(x) - min(x) >= 1 FROM t;")
    finally:
        node.query("DROP TABLE IF EXISTS t;")


def test_log_max_size(start_cluster):
    # we do misconfiguration here: buffer_size_rows_flush_threshold > max_size_rows, flush_interval_milliseconds is huge
    # no auto flush by size not by time has a chance
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <flush_interval_milliseconds replace=\\"replace\\">1000000</flush_interval_milliseconds>
                <buffer_size_rows_flush_threshold replace=\\"replace\\">1000000</buffer_size_rows_flush_threshold>
                <max_size_rows replace=\\"replace\\">10</max_size_rows>
                <reserved_size_rows replace=\\"replace\\">10</reserved_size_rows>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )

    node.query("SYSTEM FLUSH LOGS")
    node.query(f"TRUNCATE TABLE IF EXISTS system.query_log")
    node.restart_clickhouse()

    # all logs records above max_size_rows are lost
    # The accepted logs records are never flushed until system flush logs is called by us
    for i in range(21):
        node.query(f"select {i}")
    node.query("system flush logs")

    assert_logs_contain_with_retry(
        node, "Queue had been full at 0, accepted 10 logs, ignored 34 logs."
    )
    assert node.query(
        "select count() >= 10, count() < 20 from system.query_log"
    ) == TSV([[1, 1]])

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
