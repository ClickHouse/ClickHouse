import pytest

import uuid
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["config/metric_log_config.xml"],
    stay_alive=True,
)
# The `bucketed` schema adds engine settings to the default table definition, which is only
# used when the configuration does not specify `engine` explicitly, hence a separate config.
node2 = cluster.add_instance(
    "node2",
    main_configs=["config/metric_log_bucketed_config.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["config/metric_log_config.xml"],
    stay_alive=True,
)

node4 = cluster.add_instance(
    "node4",
    main_configs=["config/metric_log_config.xml", "config/storage_policy.xml"],
    with_minio=True,
    stay_alive=True,
)

node5 = cluster.add_instance(
    "node5",
    main_configs=[
        "config/metric_log_bucketed_config.xml",
        "config/skip_alias_columns.xml",
    ],
    stay_alive=True,
)

LOG_PATH = "/etc/clickhouse-server/config.d/metric_log_config.xml"
BUCKETED_LOG_PATH = "/etc/clickhouse-server/config.d/metric_log_bucketed_config.xml"

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()

def test_table_rotation(start_cluster):
    # default wide mode
    node1.query("SYSTEM FLUSH LOGS")
    assert int(node1.query("select count() from system.metric_log").strip()) > 0
    assert "ProfileEvent_Query" in node1.query("SHOW CREATE TABLE system.metric_log")

    node1.replace_in_config(LOG_PATH, ">wide<", ">transposed<")

    # transposed mode
    node1.restart_clickhouse()

    node1.query("SYSTEM FLUSH LOGS")

    assert int(node1.query("select count() from system.metric_log").strip()) > 0
    assert "metric" in node1.query("SHOW CREATE TABLE system.metric_log")
    assert "ORDER BY (event_date, event_time)" in node1.query("SHOW CREATE TABLE system.metric_log")

    assert int(node1.query("select countDistinct(metric) from system.metric_log").strip()) > 1000

    in_old_metric_log = int(node1.query("select count() from system.metric_log_0").strip())

    assert in_old_metric_log > 0

    node1.replace_in_config(LOG_PATH, ">transposed<", ">wide<")
    node1.restart_clickhouse()


def test_bucketed_schema(start_cluster):
    # default wide mode
    node2.query("SYSTEM FLUSH LOGS")
    assert int(node2.query("select count() from system.metric_log").strip()) > 0
    assert "ProfileEvent_Query" in node2.query("SHOW CREATE TABLE system.metric_log")

    node2.replace_in_config(BUCKETED_LOG_PATH, ">wide<", ">bucketed<")

    # bucketed mode: a single Map(Enum16(...), Int64) column with bucketed serialization and per-metric aliases
    node2.restart_clickhouse()

    # The public table name must resolve to the bucketed log for named flushes as well
    node2.query("SYSTEM FLUSH LOGS metric_log")
    node2.query("SYSTEM FLUSH LOGS system.metric_log")

    # `TSVRaw`: the default format escapes the quotes inside the query text
    create_query = node2.query("SHOW CREATE TABLE system.metric_log FORMAT TSVRaw")
    assert "`metrics` Map(Enum16(" in create_query
    assert "map_serialization_version = 'with_buckets'" in create_query
    assert "max_buckets_in_map = 128" in create_query
    assert "map_buckets_strategy = 'constant'" in create_query
    assert "ALIAS metrics['ProfileEvent_Query']" in create_query

    assert int(node2.query("select count() from system.metric_log").strip()) > 0
    assert int(node2.query("select max(length(metrics)) from system.metric_log").strip()) > 0
    # aliases read from the map; a missing key reads as zero
    assert int(node2.query("select sum(ProfileEvent_Query) from system.metric_log").strip()) > 0
    assert int(node2.query("select max(CurrentMetric_GlobalThread) from system.metric_log").strip()) > 0

    # the old wide table was rotated
    assert int(node2.query("select count() from system.metric_log_0").strip()) > 0

    node2.replace_in_config(BUCKETED_LOG_PATH, ">bucketed<", ">wide<")
    node2.restart_clickhouse()


def test_bucketed_schema_is_rejected_without_alias_columns(start_cluster):
    # With `default_system_log_flush_policy.skip_alias_columns` the per-metric columns of the
    # bucketed schema cannot be created, so the server must refuse to start instead of
    # silently exposing a table without the `ProfileEvent_*` / `CurrentMetric_*` columns.
    node5.query("SYSTEM FLUSH LOGS")
    assert "ProfileEvent_Query" in node5.query("SHOW CREATE TABLE system.metric_log")

    node5.replace_in_config(BUCKETED_LOG_PATH, ">wide<", ">bucketed<")
    node5.stop_clickhouse()
    node5.start_clickhouse(expected_to_fail=True)

    assert node5.contains_in_log("cannot be created without alias columns")

    node5.replace_in_config(BUCKETED_LOG_PATH, ">bucketed<", ">wide<")
    node5.start_clickhouse()


def insert_into_transposed_metric_log(node, table_name, size):
    INGEST_INTO_TRANSPOSED_LOG = f"""
    INSERT INTO system.{table_name} WITH
        (
            SELECT min(event_time)
            FROM system.{table_name}
        ) AS min_time,
        (
            SELECT groupArray(metric)
            FROM system.{table_name}
            WHERE event_time = min_time
            GROUP BY ALL
        ) AS arr
    SELECT
        hostname(),
        toDate('2024-10-01'),
        toDateTime('2024-10-01 00:00:00') + number,
        toDateTime64(toDateTime('2024-10-01 00:00:00') + number, 6),
        arrayJoin(arr),
        number
    FROM numbers({size})
    """

    node.query(INGEST_INTO_TRANSPOSED_LOG)


def insert_into_metric_log(node, table_name, size):
    total_columns = int(node.query(f"SELECT count() from system.columns where table = '{table_name}' and (name like 'ProfileEvent%' or name like 'CurrentMetric%')").strip())
    data_str = ",".join(["number"] * total_columns)

    INGEST_INTO_METRIC_LOG = """
    INSERT INTO system.{table_name}
    SELECT hostname(), toDate('2024-10-01'), toDateTime('2024-10-01 00:00:00') + number, toDateTime64(toDateTime('2024-10-01 00:00:00') + number, 6), {number_part} FROM numbers({size})
    """

    query = INGEST_INTO_METRIC_LOG.format(i="number", number_part=data_str, table_name=table_name, size=size)

    node.query(query)


def exec_query_and_print_stats(node, name, query, settings={}):
    query_id = uuid.uuid4().hex
    node.query(query, query_id=query_id, settings=settings)

    node.query("SYSTEM FLUSH LOGS")

    duration = int(node.query(f"SELECT query_duration_ms from system.query_log WHERE query_id='{query_id}' and type = 'QueryFinish'").strip())
    memory = node.query(f"SELECT formatReadableSize(memory_usage) from system.query_log WHERE query_id='{query_id}' and type = 'QueryFinish'").strip()
    result_rows = int(node.query(f"SELECT result_rows from system.query_log WHERE query_id='{query_id}' and type = 'QueryFinish'").strip())
    read_rows = int(node.query(f"SELECT read_rows from system.query_log WHERE query_id='{query_id}' and type = 'QueryFinish'").strip())

    print(f"Query '{name}': duration {duration}ms, memory {memory}, read rows {read_rows}, result rows {result_rows}")


@pytest.mark.parametrize(
    "node",[node3, node4],
)
def test_some_perf(start_cluster, node):
    pytest.skip("Perf test with no checks doesn't make sense to run in CI")
    node.query("SYSTEM FLUSH LOGS")
    assert "ProfileEvent_Query" in node.query("SHOW CREATE TABLE system.metric_log")

    node.replace_in_config(LOG_PATH, ">wide<", ">transposed_with_wide_view<")

    node.restart_clickhouse()

    node.query("SYSTEM FLUSH LOGS")

    insert_into_transposed_metric_log(node, "transposed_metric_log", 86400 * 5)
    insert_into_metric_log(node, "metric_log_0", 86400 * 5)

    exec_query_and_print_stats(node, "wide single column", "SELECT event_time, ProfileEvent_Query FROM system.metric_log_0 WHERE event_date < yesterday() order by event_time FORMAT Null")
    exec_query_and_print_stats(node, "view single column", "SELECT event_time, ProfileEvent_Query FROM system.metric_log WHERE event_date < yesterday() order by event_time FORMAT Null")

    exec_query_and_print_stats(node, "wide all column", "SELECT * FROM system.metric_log_0 WHERE event_date < yesterday() order by event_time FORMAT Null")
    exec_query_and_print_stats(node, "view all column", "SELECT * FROM system.metric_log WHERE event_date < yesterday() order by event_time FORMAT Null")

    exec_query_and_print_stats(node, "wide all column with filter", "SELECT * FROM system.metric_log_0 WHERE event_date < yesterday() and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time FORMAT Null")
    exec_query_and_print_stats(node, "view all column with filter", "SELECT * FROM system.metric_log WHERE event_date < yesterday() and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time FORMAT Null")

    exec_query_and_print_stats(node, "optimize wide table", "OPTIMIZE TABLE system.metric_log_0 FINAL")
    exec_query_and_print_stats(node, "optimize narrow table", "OPTIMIZE TABLE system.transposed_metric_log FINAL")

    exec_query_and_print_stats(node, "wide single column (max_threads = 2)", "SELECT event_time, ProfileEvent_Query FROM system.metric_log_0 WHERE event_date < yesterday() order by event_time FORMAT Null", settings={"max_threads": 2})
    exec_query_and_print_stats(node, "view single column (max_threads = 2)", "SELECT event_time, ProfileEvent_Query FROM system.metric_log WHERE event_date < yesterday() order by event_time FORMAT Null", settings={"max_threads": 2})

    exec_query_and_print_stats(node, "wide all column (max_threads = 2)", "SELECT * FROM system.metric_log_0 WHERE event_date < yesterday() order by event_time FORMAT Null", settings={"max_threads": 2})
    exec_query_and_print_stats(node, "view all column (max_threads = 2)", "SELECT * FROM system.metric_log WHERE event_date < yesterday() order by event_time FORMAT Null", settings={"max_threads": 2})

    exec_query_and_print_stats(node, "wide all column with filter (max_threads = 2)", "SELECT * FROM system.metric_log_0 WHERE event_date < yesterday() and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time FORMAT Null", settings={"max_threads": 2})
    exec_query_and_print_stats(node, "view all column with filter (max_threads = 2)", "SELECT * FROM system.metric_log WHERE event_date < yesterday() and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time FORMAT Null", settings={"max_threads": 2})
