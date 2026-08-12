import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        yield
    finally:
        node.query("DROP TABLE IF EXISTS default.prometheus SYNC")


def test_insert_basic():
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'job': 'test', 'instance': 'localhost:9090'}, [(toDateTime64(1000, 3), 0.5), (toDateTime64(2000, 3), 0.7)])"
    )

    # Check inner tables.
    assert node.query(
        "SELECT d.timestamp, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " ORDER BY d.timestamp"
    ) == TSV([
        ["1970-01-01 00:16:40.000", "0.5"],
        ["1970-01-01 00:33:20.000", "0.7"],
    ])

    assert node.query(
        "SELECT t.metric_name, t.tags"
        " FROM timeSeriesTags(prometheus) AS t"
    ) == TSV([["cpu_usage", "{'instance':'localhost:9090','job':'test'}"]])

    # Check prometheusQuery() can use the inserted data.
    assert node.query(
        "SELECT * FROM prometheusQuery(prometheus, 'cpu_usage', 2000)"
    ) == TSV([["[('__name__','cpu_usage'),('instance','localhost:9090'),('job','test')]", "1970-01-01 00:33:20.000", "0.7"]])


def test_insert_with_metrics_metadata():
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests', {'method': 'GET'}, [(toDateTime64(1000, 3), 100.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )

    # Check inner tables.
    assert node.query(
        "SELECT metric_family_name, type, unit, help"
        " FROM timeSeriesMetrics(prometheus)"
    ) == TSV([["http_requests", "counter", "requests", "Total HTTP requests"]])

    assert node.query(
        "SELECT d.value FROM timeSeriesData(prometheus) AS d"
    ) == TSV([["100"]])


def test_promql_at_timestamp_with_supported_timestamp_types():
    timestamp_types = [
        ("u32", "UInt32", "4294967196", "0", "0"),
        ("datetime", "DateTime", "toDateTime(4294967196)", "toDateTime(0)", "0"),
        ("datetime64", "DateTime64(3)", "toDateTime64(-100, 3)", "toDateTime64(0, 3)", "1"),
    ]

    for suffix, timestamp_type, wrapped_timestamp, epoch_timestamp, expected_negative_count in timestamp_types:
        table_name = f"prometheus_{suffix}"
        try:
            node.query(
                f"CREATE TABLE {table_name} (time_series Array(Tuple({timestamp_type}, Float64))) ENGINE=TimeSeries"
            )
            node.query(
                f"INSERT INTO {table_name} (metric_name, tags, time_series) VALUES"
                f" ('metric', {{'job': 'test'}}, [({wrapped_timestamp}, 1), ({epoch_timestamp}, 2)])"
            )

            assert node.query(
                f"SELECT count() FROM prometheusQuery({table_name}, 'metric @ -100', 0)"
            ) == expected_negative_count

            # The default five-minute lookback crosses the Unix epoch at @ 100.
            assert node.query(
                f"SELECT count() FROM prometheusQuery({table_name}, 'metric @ 100', 0)"
            ) == "1"
        finally:
            node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
