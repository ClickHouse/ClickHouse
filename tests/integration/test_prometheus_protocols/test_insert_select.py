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


def insert_three_series():
    """Helper used by the SELECT tests below: inserts three distinct time series."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests_total', {'job': 'api', 'instance': 'a'}, [(toDateTime64(1000, 3), 1.0)]),"
        " ('http_requests_total', {'job': 'api', 'instance': 'b'}, [(toDateTime64(2000, 3), 2.0)]),"
        " ('cpu_usage',           {'host': 'h1'},                   [(toDateTime64(3000, 3), 0.5)])"
    )


def test_select_metric_name_and_tags():
    insert_three_series()

    assert node.query(
        "SELECT metric_name, tags FROM prometheus ORDER BY metric_name, tags"
    ) == TSV([
        ["cpu_usage",           "{'host':'h1'}"],
        ["http_requests_total", "{'instance':'a','job':'api'}"],
        ["http_requests_total", "{'instance':'b','job':'api'}"],
    ])


def test_select_only_metric_name():
    insert_three_series()

    assert node.query(
        "SELECT metric_name FROM prometheus ORDER BY metric_name"
    ) == TSV([
        ["cpu_usage"],
        ["http_requests_total"],
        ["http_requests_total"],
    ])


def test_select_only_tags():
    insert_three_series()

    assert node.query(
        "SELECT tags FROM prometheus ORDER BY tags"
    ) == TSV([
        ["{'host':'h1'}"],
        ["{'instance':'a','job':'api'}"],
        ["{'instance':'b','job':'api'}"],
    ])


def test_select_count():
    insert_three_series()

    # `count()` over the engine returns the number of time series (one per tags row).
    assert node.query("SELECT count() FROM prometheus") == "3\n"


def test_select_with_where_on_metric_name():
    """WHERE-only columns must be passed through to the inner SELECT even when they're not in
    the SELECT list. Here `metric_name` is in WHERE, not in SELECT, and the filter still works."""
    insert_three_series()

    assert node.query(
        "SELECT count() FROM prometheus WHERE metric_name = 'cpu_usage'"
    ) == "1\n"
    assert node.query(
        "SELECT count() FROM prometheus WHERE metric_name = 'http_requests_total'"
    ) == "2\n"


def test_select_with_where_on_tags_map_element():
    """A predicate that pokes into the tags Map exercises the `tags` column being in WHERE only."""
    insert_three_series()

    assert node.query(
        "SELECT count() FROM prometheus WHERE tags['job'] = 'api'"
    ) == "2\n"
    assert node.query(
        "SELECT count() FROM prometheus WHERE tags['host'] = 'h1'"
    ) == "1\n"
