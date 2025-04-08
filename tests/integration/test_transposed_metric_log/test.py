import pytest

import uuid
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["config/metric_log_config.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["config/metric_log_config.xml"],
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

LOG_PATH = "/etc/clickhouse-server/config.d/metric_log_config.xml"

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

    assert int(node1.query("select countDistinct(metric) from system.metric_log").strip()) > 1000

    in_old_metric_log = int(node1.query("select count() from system.metric_log_0").strip())

    assert in_old_metric_log > 0

    time.sleep(1)
    node1.query("SYSTEM FLUSH LOGS")

    in_old_metric_log_again = int(node1.query("select count() from system.metric_log_0").strip())

    assert in_old_metric_log == in_old_metric_log_again

    node1.replace_in_config(LOG_PATH, ">transposed<", ">transposed_with_wide_view<")

    # transposed with wide view
    node1.restart_clickhouse()

    time.sleep(1)
    node1.query("SYSTEM FLUSH LOGS")

    assert int(node1.query("select count() from system.metric_log").strip()) > 0
    assert int(node1.query("select count() from system.transposed_metric_log").strip()) > 0

    assert "metric" in node1.query("SHOW CREATE TABLE system.transposed_metric_log")
    assert "ProfileEvent_Query" in node1.query("SHOW CREATE TABLE system.metric_log")

    in_old_metric_log_again = int(node1.query("select count() from system.metric_log_0").strip())
    in_older_metric_log_again = int(node1.query("select count() from system.metric_log_1").strip())

    assert in_old_metric_log_again > 0
    assert in_older_metric_log_again > 0

    time.sleep(1)
    node1.query("SYSTEM FLUSH LOGS")

    assert int(node1.query("select count() from system.metric_log_0").strip()) == in_old_metric_log_again
    assert int(node1.query("select count() from system.metric_log_1").strip()) == in_older_metric_log_again

    # back to wide mode again
    node1.replace_in_config(LOG_PATH, ">transposed_with_wide_view<", ">wide<")
    node1.restart_clickhouse()

    time.sleep(1)
    node1.query("SYSTEM FLUSH LOGS")

    assert int(node1.query("select count() from system.metric_log").strip()) > 0
    assert "ProfileEvent_Query" in node1.query("SHOW CREATE TABLE system.metric_log")
    transposed_counter = int(node1.query("select count() from system.transposed_metric_log").strip())

    time.sleep(1)
    node1.query("SYSTEM FLUSH LOGS")

    assert int(node1.query("select count() from system.transposed_metric_log").strip()) == transposed_counter

    assert int(node1.query("EXISTS TABLE system.metric_log_2").strip()) == 0


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

def test_data_correctness(start_cluster):
    node2.query("SYSTEM FLUSH LOGS")
    assert "ProfileEvent_Query" in node2.query("SHOW CREATE TABLE system.metric_log")

    node2.replace_in_config(LOG_PATH, ">wide<", ">transposed_with_wide_view<")

    # transposed mode
    node2.restart_clickhouse()

    node2.query("SYSTEM FLUSH LOGS")

    insert_into_transposed_metric_log(node2, "transposed_metric_log", 20000)
    insert_into_metric_log(node2, "metric_log_0", 20000)

    old_data = int(node2.query("SELECT count() FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01')").strip())
    new_data = int(node2.query("SELECT count() FROM system.metric_log WHERE event_date == toDate('2024-10-01')").strip())
    assert new_data == old_data

    old_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 1"))
    new_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 1"))

    assert old_data == new_data

    old_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 2000,4053"))
    new_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 2000,4053"))

    assert old_data == new_data

    old_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time"))
    new_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query FROM system.metric_log WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time"))

    assert old_data == new_data

    old_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query, CurrentMetric_Query, ProfileEvent_FileSegmentLockMicroseconds, ProfileEvent_MarkCacheHits FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time"))
    new_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query, CurrentMetric_Query, ProfileEvent_FileSegmentLockMicroseconds, ProfileEvent_MarkCacheHits FROM system.metric_log WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time"))

    assert old_data == new_data

    old_data = TSV(node2.query("SELECT * FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 1"))
    new_data = TSV(node2.query("SELECT * FROM system.metric_log WHERE event_date == toDate('2024-10-01') order by event_time LIMIT 1"))

    assert old_data == new_data

    old_data = TSV(node2.query("select sum(h) from (select cityHash64(*) as h FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') order by event_time) group by all"))
    new_data = TSV(node2.query("select sum(h) from (select cityHash64(*) as h FROM system.metric_log WHERE event_date == toDate('2024-10-01') order by event_time) group by all"))

    assert old_data == new_data

    old_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query, CurrentMetric_Query, ProfileEvent_FileSegmentLockMicroseconds, ProfileEvent_MarkCacheHits FROM system.metric_log_0 WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time desc"))
    new_data = TSV(node2.query("SELECT event_time, ProfileEvent_Query, CurrentMetric_Query, ProfileEvent_FileSegmentLockMicroseconds, ProfileEvent_MarkCacheHits FROM system.metric_log WHERE event_date == toDate('2024-10-01') and event_time between toDateTime('2024-10-01 07:13:44') and toDateTime('2024-10-01 09:59:59') order by event_time desc"))

    assert old_data == new_data

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
