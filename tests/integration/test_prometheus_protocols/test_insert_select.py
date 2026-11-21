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
