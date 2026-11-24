# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_default",
    main_configs=[
        "configs/config.xml",
    ],
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
        ("system.query_metric_log", 1),
        ("system.query_thread_log", 1),
        ("system.part_log", 1),
        ("system.trace_log", 1),
        ("system.metric_log", 1),
        ("system.error_log", 1),
    ]

    for table, exists in system_logs:
        node.query(f"SYSTEM FLUSH LOGS {table}")
        q = f"SELECT * FROM {table}"
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
            """echo "
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
    node.query("TRUNCATE TABLE IF EXISTS system.query_log")
    node.restart_clickhouse()

    # all logs records above max_size_rows are lost
    # The accepted logs records are never flushed until system flush logs is called by us
    for i in range(21):
        node.query(f"select {i}")
    node.query("system flush logs")

    assert_logs_contain_with_retry(
        node, "Queue had been full at 0, accepted 10 logs, ignored 34 logs."
    )
    # The same batch is added to the counter. Later flushes can drop more rows.
    assert_eq_with_retry(
        node,
        "SELECT sum(value) >= 34 FROM system.dimensional_metrics "
        "WHERE metric = 'system_log_dropped_rows_total' "
        "AND labels['database'] = 'system' AND labels['table'] = 'query_log'",
        "1",
    )
    assert node.query(
        "select count() >= 10, count() < 20 from system.query_log"
    ) == TSV([[1, 1]])

    node.exec_in_container(
        ["rm", "/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )


def test_log_buffer_size_rows_flush_threshold(start_cluster):
    node.exec_in_container(
        [
            "bash",
            "-c",
            """echo "
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
    node.query("TRUNCATE TABLE IF EXISTS system.query_log")
    for i in range(10):
        node.query(f"select {i}")

    assert_eq_with_retry(
        node,
        "select count() >= 11 from system.query_log",
        "1",
        sleep_time=0.2,
        retry_count=100,
    )

    node.query("TRUNCATE TABLE IF EXISTS system.query_log")
    node.exec_in_container(
        [
            "bash",
            "-c",
            """echo "
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
        "select count() < 10 from system.query_log",
        "1",
        sleep_time=0.2,
        retry_count=100,
    )

    node.exec_in_container(
        ["rm", "/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )


def test_system_warnings(start_cluster):
    if node.is_debug_build():
        assert node.query("SELECT count() > 1 FROM system.warnings") == "1\n"

    node.query("TRUNCATE TABLE system.warnings")
    assert node.query("SELECT count() FROM system.warnings") == "0\n"


def get_query_log_flush_counters():
    def get(metric, result=None):
        result_filter = f"AND labels['result'] = '{result}'" if result else ""
        return float(
            node.query(
                f"SELECT sum(value) FROM system.dimensional_metrics WHERE metric = '{metric}' "
                f"AND labels['database'] = 'system' AND labels['table'] = 'query_log' {result_filter}"
            )
        )

    return (
        get("system_log_flushes_total", "success"),
        get("system_log_flushes_total", "failure"),
        get("system_log_flushed_rows_total"),
    )


def test_flush_error_is_counted(start_cluster):
    node.query("SYSTEM ENABLE FAILPOINT system_log_flush_throw")
    try:
        # A flush that started before the failpoint was enabled ends before this one.
        node.query("SELECT 1")
        node.query_and_get_answer_with_error("SYSTEM FLUSH LOGS query_log")
        success_before, failure_before, rows_before = get_query_log_flush_counters()

        node.query("SELECT 1")
        # The failed flush is only logged by the server, but an error here is fine too.
        node.query_and_get_answer_with_error("SYSTEM FLUSH LOGS query_log")
        success_after, failure_after, rows_after = get_query_log_flush_counters()
    finally:
        node.query("SYSTEM DISABLE FAILPOINT system_log_flush_throw")

    assert failure_after >= failure_before + 1
    assert success_after == success_before
    assert rows_after == rows_before

    # The flush works again without the failpoint.
    node.query("SELECT 1")
    node.query("SYSTEM FLUSH LOGS query_log")
    assert get_query_log_flush_counters()[0] > success_after
