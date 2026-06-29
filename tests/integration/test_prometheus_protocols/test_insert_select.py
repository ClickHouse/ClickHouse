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
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','instance':'a','job':'api'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','instance':'b','job':'api'}"],
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
        ["{'__name__':'cpu_usage','host':'h1'}"],
        ["{'__name__':'http_requests_total','instance':'a','job':'api'}"],
        ["{'__name__':'http_requests_total','instance':'b','job':'api'}"],
    ])


def test_select_tags_with_name_in_tags_map():
    """The metric name may live in the inner tags Map under `__name__` (e.g. a row inserted directly into
    the inner tags table with an empty `metric_name` column). The reconstructed `tags` must use that
    `__name__` (without treating the empty `metric_name` column as a conflicting tag), and the outer
    `metric_name` column falls back to that `__name__`."""
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES"
        " ('', {'__name__': 'bar', 'x': '1'})"
    )

    assert node.query("SELECT metric_name, tags FROM prometheus") == TSV([["bar", "{'__name__':'bar','x':'1'}"]])


def test_select_where_metric_name_pushdown_keeps_name_in_tags():
    """A `metric_name` condition is pushed onto the tags scan as `metric_name IN ('', <consts>)`. The empty
    string keeps series whose name lives in the tags Map (empty `metric_name` column), so such a series is
    still returned when filtered by its name."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu', {'a': '1'}, [(toDateTime64(1000, 3), 1.0)])"
    )
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES"
        " ('', {'__name__': 'cpu', 'z': '9'})"
    )

    # Both series have metric name 'cpu' (one in the column, one in the tags Map) and must be returned.
    assert node.query("SELECT count() FROM prometheus WHERE metric_name = 'cpu'") == "2\n"
    assert node.query("SELECT count() FROM prometheus WHERE metric_name = 'mem'") == "0\n"


def test_select_where_promoted_tag_pushdown():
    """A condition on a tag promoted to its own column (`tags_to_columns`) is pushed onto that column on the
    tags scan as `<col> IN ('', <consts>)`. The empty string keeps a series whose tag is instead stored in
    the `tags` Map (empty column), so it is still returned."""
    node.query("DROP TABLE IF EXISTS prom_promoted SYNC")
    node.query("CREATE TABLE prom_promoted ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_promoted (metric_name, tags, time_series) VALUES"
            " ('cpu', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)]),"
            " ('mem', {'job': 'web'}, [(toDateTime64(2000, 3), 2.0)])"
        )
        # A series whose `job` is stored in the tags Map (empty `job` column), inserted into the inner table.
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_promoted) (metric_name, tags) VALUES"
            " ('direct', {'job': 'api', 'x': '1'})"
        )

        # 'cpu' (job in column) and 'direct' (job in the Map) both match; 'mem' does not.
        assert node.query("SELECT count() FROM prom_promoted WHERE tags['job'] = 'api'") == "2\n"
        assert node.query("SELECT count() FROM prom_promoted WHERE tags['job'] = 'web'") == "1\n"
    finally:
        node.query("DROP TABLE IF EXISTS prom_promoted SYNC")


def test_select_where_selector_match_tags_pushdown():
    """A `WHERE timeSeriesSelectorMatchTags('<selector>', tags)` filter has the equality matchers of its
    constant PromQL selector pushed onto the tags scan (`__name__` -> the metric_name column, a promoted tag
    -> its column), each as `<col> IN ('', <consts>)`. The empty string keeps series whose value lives in the
    `tags` Map, so they are still returned. Regex and negative matchers can't use the index, but the outer
    function call still enforces the whole selector exactly."""
    node.query("DROP TABLE IF EXISTS prom_selector SYNC")
    node.query("CREATE TABLE prom_selector ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_selector (metric_name, tags, time_series) VALUES"
            " ('cpu', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)]),"
            " ('mem', {'job': 'web'}, [(toDateTime64(2000, 3), 2.0)])"
        )
        # A series whose name and `job` both live in the tags Map (empty `metric_name`/`job` columns).
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_selector) (metric_name, tags) VALUES"
            " ('', {'__name__': 'cpu', 'job': 'api'})"
        )

        # `__name__` matcher: 'cpu' in the column and 'cpu' in the Map both match; 'mem' does not.
        assert node.query(
            "SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{__name__=\"cpu\"}', tags)"
        ) == "2\n"
        # Metric name plus a promoted-tag matcher.
        assert node.query(
            "SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('cpu{job=\"api\"}', tags)"
        ) == "2\n"
        # A promoted-tag matcher alone keeps the column-stored 'mem' series only.
        assert node.query(
            "SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{job=\"web\"}', tags)"
        ) == "1\n"
        # A regex matcher isn't pushed down but is still enforced exactly by the outer call.
        assert node.query(
            "SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{__name__=~\"c.*\"}', tags)"
        ) == "2\n"
    finally:
        node.query("DROP TABLE IF EXISTS prom_selector SYNC")


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


def test_select_only_time_series():
    """Bare samples branch: time_series alone, no Tags JOIN. Each row is one time series'
    grouped (timestamp, value) tuples."""
    insert_three_series()

    # Three time series → three rows; sort by length so the assertion is order-independent.
    assert node.query(
        "SELECT length(time_series) AS n FROM prometheus ORDER BY n"
    ) == TSV([["1"], ["1"], ["1"]])


def test_select_metric_name_and_time_series():
    """tags + samples branch: SEMI LEFT JOIN with the samples sub-aggregate."""
    # Insert a series with multiple samples so we can also assert on aggregation correctness.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('m', {'k':'1'}, [(toDateTime64(1000, 3), 1.0), (toDateTime64(1500, 3), 1.5)]),"
        " ('m', {'k':'2'}, [(toDateTime64(2000, 3), 2.0)])"
    )

    assert node.query(
        "SELECT metric_name, tags, length(time_series) AS n FROM prometheus ORDER BY tags"
    ) == TSV([
        ["m", "{'__name__':'m','k':'1'}", "2"],
        ["m", "{'__name__':'m','k':'2'}", "1"],
    ])


def test_select_full_time_series_array():
    """Sanity: the time_series array carries the actual (timestamp, value) tuples."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('m', {}, [(toDateTime64(1000, 3), 1.5), (toDateTime64(2000, 3), 2.5)])"
    )

    assert node.query(
        "SELECT time_series FROM prometheus"
    ) == TSV([
        ["[('1970-01-01 00:16:40.000',1.5),('1970-01-01 00:33:20.000',2.5)]"],
    ])


def insert_two_metric_families():
    """Helper for the metrics tests: two series in two metric families, each carrying metadata."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'},  [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests'),"
        " ('memory_bytes',         {'host': 'h1'}, [(toDateTime64(2000, 3), 5.0)], 'memory_bytes',  'gauge',   'bytes',    'Memory usage')"
    )


def test_select_only_metrics_metadata():
    """metrics-only branch: reads the metrics table, exposing inner `metric_family_name` as `metric_family`.
    One row per metric family, independent of how many time series belong to it."""
    insert_two_metric_families()

    assert node.query(
        "SELECT metric_family, type, unit, help FROM prometheus ORDER BY metric_family"
    ) == TSV([
        ["http_requests", "counter", "requests", "Total HTTP requests"],
        ["memory_bytes",  "gauge",   "bytes",    "Memory usage"],
    ])

    # A single metrics column also works.
    assert node.query(
        "SELECT type FROM prometheus ORDER BY type"
    ) == TSV([["counter"], ["gauge"]])


def test_select_tags_and_metrics_metadata():
    """tags + metrics branch: each time series is joined to its metric family's metadata via the
    family computed from its metric name. Several series in one family share the same metadata row."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    # A second series in the same family ('_total' stripped to 'http_requests'), inserted without metadata.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests_total', {'job': 'web'}, [(toDateTime64(2000, 3), 2.0)])"
    )

    assert node.query(
        "SELECT metric_name, tags, metric_family, type, unit, help FROM prometheus ORDER BY tags"
    ) == TSV([
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "http_requests", "counter", "requests", "Total HTTP requests"],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'web'}", "http_requests", "counter", "requests", "Total HTTP requests"],
    ])


def test_select_tags_and_metrics_missing_metadata():
    """The FULL join keeps time series whose metric family has no metadata row; their metadata
    columns come back empty (the row is not dropped)."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'host': 'h1'}, [(toDateTime64(1000, 3), 0.5)])"
    )

    assert node.query(
        "SELECT metric_name, type, unit, help FROM prometheus"
    ) == TSV([["cpu_usage", "", "", ""]])


def test_select_tags_and_metrics_family_without_series():
    """The FULL join also keeps metric families that have metadata but no stored time series; their
    metric_name and tags come back empty."""
    # A normal series together with its family's metadata.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    # Metadata for a family with no time series at all: only metrics columns are inserted, so nothing
    # is written to the tags table.
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('memory_bytes', 'gauge', 'bytes', 'Memory usage')"
    )

    assert node.query(
        "SELECT metric_name, tags, metric_family, type, unit, help FROM prometheus ORDER BY metric_family"
    ) == TSV([
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "http_requests", "counter", "requests", "Total HTTP requests"],
        ["",                    "{}",            "memory_bytes",  "gauge",   "bytes",    "Memory usage"],
    ])


def test_select_metrics_metadata_deduplicated():
    """The metrics metadata is deduplicated per family: the metrics table may hold several not-yet-merged
    rows for one family (its engine isn't guaranteed to deduplicate on read), so each family appears once."""
    # Two metadata rows for the same family (only `help` differs).
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('http_requests', 'counter', 'requests', 'First help')"
    )
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('http_requests', 'counter', 'requests', 'Second help')"
    )

    # One row, not two (the family is collapsed by aggregation).
    assert node.query(
        "SELECT metric_family, type, unit FROM prometheus"
    ) == TSV([["http_requests", "counter", "requests"]])


def test_select_tags_and_metrics_deduplicated():
    """A metric family duplicated in the metrics table must not multiply its matching time series:
    the metadata is deduplicated before the FULL JOIN."""
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('http_requests', 'counter', 'requests', 'First help')"
    )
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('http_requests', 'counter', 'requests', 'Second help')"
    )
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)])"
    )

    # One row for the single series, not two.
    assert node.query(
        "SELECT metric_name, tags, metric_family, type, unit FROM prometheus"
    ) == TSV([["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "http_requests", "counter", "requests"]])


def test_select_time_series_and_metrics():
    """`time_series` together with metrics metadata, without selecting any tags column: the "tags" table
    is still read internally to bridge samples (by id) and metrics (by metric family)."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0), (toDateTime64(2000, 3), 2.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )

    assert node.query(
        "SELECT length(time_series) AS n, type, unit FROM prometheus"
    ) == TSV([["2", "counter", "requests"]])


def test_select_tags_samples_and_metrics():
    """All three target tables at once. A time series that has samples is emitted with its family's
    metadata (matched) or with empty metadata (its family has none); a metric family with no time series
    is emitted with empty metric_name/tags and an empty time_series."""
    # Series with samples whose family has metadata.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    # Series with samples whose family has no metadata.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'host': 'h1'}, [(toDateTime64(2000, 3), 0.5)])"
    )
    # Metadata for a family that has no time series.
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('memory_bytes', 'gauge', 'bytes', 'Memory usage')"
    )

    assert node.query(
        "SELECT metric_name, tags, length(time_series) AS n, metric_family, type"
        " FROM prometheus ORDER BY metric_name, metric_family"
    ) == TSV([
        ["",                    "{}",                                   "0", "memory_bytes",  "gauge"],
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}", "1", "",              ""],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "1", "http_requests", "counter"],
    ])
