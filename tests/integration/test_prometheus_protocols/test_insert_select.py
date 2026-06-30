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
    """The `metric_name` and `tags` outer columns are reconstructed from the tags table — together, and each
    on its own (single-column tags-table reads)."""
    insert_three_series()

    # `count()` over the engine returns the number of time series (one tags row per series).
    assert node.query("SELECT count() FROM prometheus") == "3\n"

    assert node.query(
        "SELECT metric_name, tags FROM prometheus ORDER BY metric_name, tags"
    ) == TSV([
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','instance':'a','job':'api'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','instance':'b','job':'api'}"],
    ])
    assert node.query("SELECT metric_name FROM prometheus ORDER BY metric_name") == TSV([
        ["cpu_usage"], ["http_requests_total"], ["http_requests_total"],
    ])
    assert node.query("SELECT tags FROM prometheus ORDER BY tags") == TSV([
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


def test_select_where_metric_name_pushdown():
    """A `metric_name` condition is pushed onto the tags scan (`metric_name` is the leading primary-key column):
    equality as `metric_name IN ('', <consts>)`, and `startsWith`/`LIKE 'p%'`/`match('^p')` as
    `<fn>(metric_name, ...) OR metric_name = ''`. The empty-string arm keeps a series whose name lives in the tags
    Map (empty `metric_name` column), so it is still returned — both by exact name and by prefix."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {}, [(toDateTime64(1, 3), 1)]),"
        " ('cpu_load',  {}, [(toDateTime64(1, 3), 1)]),"
        " ('mem_free',  {}, [(toDateTime64(1, 3), 1)])"
    )
    # A series whose name is in the tags Map (empty `metric_name` column).
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES ('', {'__name__': 'cpu_temp'})"
    )
    # Equality: a Map-stored name is matched via the '' arm, a column-stored name directly, a non-match yields 0.
    assert node.query("SELECT count() FROM prometheus WHERE metric_name = 'cpu_temp'") == "1\n"
    assert node.query("SELECT count() FROM prometheus WHERE metric_name = 'cpu_usage'") == "1\n"
    assert node.query("SELECT count() FROM prometheus WHERE metric_name = 'nope'") == "0\n"
    # Prefix forms keep every cpu_* series, including the Map-name one.
    expected = TSV([["cpu_load"], ["cpu_temp"], ["cpu_usage"]])
    assert node.query("SELECT metric_name FROM prometheus WHERE startsWith(metric_name, 'cpu') ORDER BY metric_name") == expected
    assert node.query("SELECT metric_name FROM prometheus WHERE metric_name LIKE 'cpu%' ORDER BY metric_name") == expected
    assert node.query("SELECT metric_name FROM prometheus WHERE match(metric_name, '^cpu') ORDER BY metric_name") == expected


def test_select_where_promoted_tag_pushdown():
    """A condition on a tag promoted to its own column (`tags_to_columns`) is pushed onto that column on the tags
    scan: equality as `<col> IN ('', <consts>)`, and prefix forms as `<fn>(<col>, ...) OR <col> = ''`. The
    empty-string arm keeps a series whose tag is instead stored in the `tags` Map (empty column)."""
    node.query("DROP TABLE IF EXISTS prom_promoted SYNC")
    node.query("CREATE TABLE prom_promoted ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_promoted (metric_name, tags, time_series) VALUES"
            " ('m1', {'job': 'api'},  [(toDateTime64(1, 3), 1)]),"
            " ('m2', {'job': 'apex'}, [(toDateTime64(1, 3), 1)]),"
            " ('m3', {'job': 'web'},  [(toDateTime64(1, 3), 1)])"
        )
        # Series whose `job` is stored in the tags Map (empty `job` column), inserted into the inner table.
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_promoted) (metric_name, tags) VALUES"
            " ('m4', {'job': 'apidoc'}), ('m5', {'job': 'api'})"
        )
        # Equality: 'api' matches the column ('m1') and the Map ('m5'); 'web' matches one.
        assert node.query("SELECT count() FROM prom_promoted WHERE tags['job'] = 'api'") == "2\n"
        assert node.query("SELECT count() FROM prom_promoted WHERE tags['job'] = 'web'") == "1\n"
        # Prefix forms match every ap* job (column- and Map-stored); 'web' excluded.
        expected = TSV([["apex"], ["api"], ["apidoc"]])  # lexicographic ('apex' < 'api')
        assert node.query("SELECT DISTINCT tags['job'] FROM prom_promoted WHERE startsWith(tags['job'], 'ap') ORDER BY 1") == expected
        assert node.query("SELECT DISTINCT tags['job'] FROM prom_promoted WHERE tags['job'] LIKE 'ap%' ORDER BY 1") == expected
        assert node.query("SELECT DISTINCT tags['job'] FROM prom_promoted WHERE match(tags['job'], '^ap') ORDER BY 1") == expected
    finally:
        node.query("DROP TABLE IF EXISTS prom_promoted SYNC")


def test_select_where_selector_pushdown():
    """A `WHERE timeSeriesSelectorMatchTags('<selector>', tags)` filter has its constant PromQL selector's matchers
    pushed onto the tags scan (`__name__` -> the metric_name column, a promoted tag -> its column). An equality
    matcher becomes `<col> IN ('', <consts>)`; a regex matcher that is an alternation of literals (e.g.
    `job=~'api|server'`) also becomes an `IN` set, while a general regex becomes a `match`. The '' arm keeps
    values stored in the `tags` Map; the outer call still enforces the whole selector exactly (so `api|server`,
    being fully anchored, is exactly "api" or "server", not "apidoc")."""
    node.query("DROP TABLE IF EXISTS prom_selector SYNC")
    node.query("CREATE TABLE prom_selector ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_selector (metric_name, tags, time_series) VALUES"
            " ('cpu',      {'job': 'api'},    [(toDateTime64(1, 3), 1)]),"
            " ('cpu_load', {'job': 'server'}, [(toDateTime64(1, 3), 1)]),"
            " ('mem_free', {'job': 'web'},    [(toDateTime64(1, 3), 1)])"
        )
        # Series whose name and/or `job` live in the tags Map (empty columns).
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_selector) (metric_name, tags) VALUES"
            " ('', {'__name__': 'cpu', 'job': 'api'}), ('cpu_temp', {'job': 'apidoc'})"
        )
        # Equality matchers: name 'cpu' matches the column series and the Map series; matchers combine; a
        # promoted-tag matcher alone selects the 'web' series.
        assert node.query("SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{__name__=\"cpu\"}', tags)") == "2\n"
        assert node.query("SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('cpu{job=\"api\"}', tags)") == "2\n"
        assert node.query("SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{job=\"web\"}', tags)") == "1\n"
        # Regex matchers: an alternation of literals (exactly api/server, anchored -> 'apidoc' excluded) and a
        # general prefix regex on the name (matches every cpu* series, column- and Map-stored).
        assert node.query("SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{job=~\"api|server\"}', tags)") == "3\n"
        assert node.query("SELECT count() FROM prom_selector WHERE timeSeriesSelectorMatchTags('{__name__=~\"cpu.*\"}', tags)") == "4\n"
    finally:
        node.query("DROP TABLE IF EXISTS prom_selector SYNC")


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
    """Bare samples branch (time_series alone, no Tags JOIN): one row per series, each carrying its grouped
    (timestamp, value) tuples in order."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('m', {}, [(toDateTime64(1000, 3), 1.5), (toDateTime64(2000, 3), 2.5)]),"
        " ('m', {'k': '1'}, [(toDateTime64(3000, 3), 0.5)])"
    )

    # Two series -> two rows; the array carries the actual (timestamp, value) tuples.
    assert node.query(
        "SELECT time_series FROM prometheus ORDER BY length(time_series)"
    ) == TSV([
        ["[('1970-01-01 00:50:00.000',0.5)]"],
        ["[('1970-01-01 00:16:40.000',1.5),('1970-01-01 00:33:20.000',2.5)]"],
    ])


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


def insert_two_metric_families():
    """Helper for the metrics tests: two series in two metric families, each carrying metadata."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'},  [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests'),"
        " ('memory_bytes',         {'host': 'h1'}, [(toDateTime64(2000, 3), 5.0)], 'memory_bytes',  'gauge',   'bytes',    'Memory usage')"
    )


def test_select_only_metrics_metadata():
    """metrics-only branch: reads the metrics table, exposing inner `metric_family_name` as `metric_family`.
    One row per metric family (independent of how many time series belong to it), deduplicated by aggregation
    since the metrics engine isn't guaranteed to merge duplicate rows on read."""
    insert_two_metric_families()
    # A duplicate metadata row for one family must collapse to a single row.
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )

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


def test_select_tags_and_metrics():
    """tags + metrics branch (each series joined to its family's metadata via the family computed from its
    metric name). Exercises every facet of the FULL join at once: metadata shared by several series in one
    family; a series whose family has no metadata (empty metadata columns, row kept); and a metric family with
    no time series (empty metric_name/tags, row kept)."""
    # A series with its family's metadata.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    # A second series in the same family ('_total' stripped to 'http_requests'), without its own metadata:
    # it shares the family's metadata row.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests_total', {'job': 'web'}, [(toDateTime64(2000, 3), 2.0)])"
    )
    # A series whose family ('cpu_usage') has no metadata at all.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'host': 'h1'}, [(toDateTime64(3000, 3), 0.5)])"
    )
    # Metadata for a family with no time series at all (only metrics columns inserted).
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('memory_bytes', 'gauge', 'bytes', 'Memory usage')"
    )

    assert node.query(
        "SELECT metric_name, tags, metric_family, type, unit, help FROM prometheus"
        " ORDER BY metric_name, metric_family, tags"
    ) == TSV([
        ["",                    "{}",                                             "memory_bytes",  "gauge",   "bytes",    "Memory usage"],
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}",           "",              "",        "",         ""],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "http_requests", "counter", "requests", "Total HTTP requests"],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'web'}", "http_requests", "counter", "requests", "Total HTTP requests"],
    ])


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


def _insert_two_series_with_timestamps():
    # 'cpu' has samples at 1000/2000/3000s; 'mem' has samples at 5000/6000s.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu', {'host': 'a'}, [(toDateTime64(1000,3),1.0),(toDateTime64(2000,3),2.0),(toDateTime64(3000,3),3.0)]),"
        " ('mem', {'host': 'b'}, [(toDateTime64(5000,3),5.0),(toDateTime64(6000,3),6.0)])"
    )


def test_select_where_timestamp_filter():
    """`timestamp` is a filter-only virtual column: it never appears in the output, but a condition on it
    selects the time range. The condition is pushed onto the samples table — any deterministic predicate over
    `timestamp` alone (`>=`/`<=`/BETWEEN/`!=`/function-of-timestamp) — reshaping each series' `time_series`
    array to the matching samples and dropping series that have no matching sample."""
    _insert_two_series_with_timestamps()
    # Reshape: 'cpu' keeps the samples at 2000 and 3000.
    assert node.query(
        "SELECT length(time_series) FROM prometheus WHERE metric_name='cpu' AND timestamp >= toDateTime64(2000,3)"
    ) == "2\n"
    # BETWEEN (-> `>=` AND `<=`): only the sample at 2000 survives.
    assert node.query(
        "SELECT length(time_series) FROM prometheus"
        " WHERE metric_name='cpu' AND timestamp BETWEEN toDateTime64(1500,3) AND toDateTime64(2500,3)"
    ) == "1\n"
    # Prune series: only 'cpu' has a sample at or before 3000; only 'mem' at or after 5000.
    assert node.query(
        "SELECT metric_name FROM prometheus WHERE timestamp <= toDateTime64(3000,3) ORDER BY metric_name"
    ) == "cpu\n"
    assert node.query(
        "SELECT count() FROM prometheus WHERE timestamp >= toDateTime64(5000,3)"
    ) == "1\n"
    # `!=` and a function of `timestamp` are pushed verbatim too (both keep cpu's 1000 and 3000).
    assert node.query(
        "SELECT length(time_series) FROM prometheus WHERE metric_name='cpu' AND timestamp != toDateTime64(2000,3)"
    ) == "2\n"
    assert node.query(
        "SELECT length(time_series) FROM prometheus"
        " WHERE metric_name='cpu' AND toUnixTimestamp64Milli(timestamp) >= 2000000"
    ) == "2\n"


def test_select_where_timestamp_without_min_max_columns():
    """The samples-table time filter must work even when the tags table doesn't store `min_time`/`max_time`
    (so the tags-side range pruning is unavailable)."""
    node.query("DROP TABLE IF EXISTS prom_no_minmax SYNC")
    node.query("CREATE TABLE prom_no_minmax ENGINE=TimeSeries SETTINGS store_min_time_and_max_time=0")
    try:
        node.query(
            "INSERT INTO prom_no_minmax (metric_name, tags, time_series) VALUES"
            " ('cpu', {}, [(toDateTime64(1000,3),1.0),(toDateTime64(2000,3),2.0)])"
        )
        assert node.query(
            "SELECT length(time_series) FROM prom_no_minmax WHERE timestamp >= toDateTime64(2000,3)"
        ) == "1\n"
    finally:
        node.query("DROP TABLE IF EXISTS prom_no_minmax SYNC")


def test_select_where_timestamp_mixed_with_other_column_rejected():
    """A `timestamp` condition that also depends on another column can't become a samples-table predicate, so
    it is rejected rather than silently returning wrong results."""
    _insert_two_series_with_timestamps()
    assert "NOT_IMPLEMENTED" in node.query_and_get_error(
        "SELECT count() FROM prometheus WHERE timestamp > toDateTime64(2000,3) OR metric_name = 'cpu'"
    )


def test_select_tag_projection_uses_reduced_tags():
    """When the query reads `tags` only as `tags['<key>']`, the `tags` column is built containing just those
    keys (resolved directly from their source) instead of the full normalized Map. The result must match the
    full reconstruction: a promoted tag comes from its column or, when absent, the Map; `__name__` from the
    metric name; any other tag from the Map; a missing tag is the empty string."""
    node.query("DROP TABLE IF EXISTS prom_proj SYNC")
    node.query("CREATE TABLE prom_proj ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_proj (metric_name, tags, time_series) VALUES"
            " ('cpu', {'job': 'api', 'host': 'h1'}, [(toDateTime64(1000,3),1.0)]),"
            " ('mem', {'job': 'web', 'host': 'h2'}, [(toDateTime64(2000,3),2.0)])"
        )
        # A series whose `job` lives in the tags Map (empty `job` column), inserted into the inner table.
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_proj) (metric_name, tags) VALUES"
            " ('direct', {'job': 'batch', 'host': 'h3'})"
        )

        # Promoted tag: values from the column ('api','web') and from the Map ('batch').
        assert node.query("SELECT DISTINCT tags['job'] FROM prom_proj ORDER BY tags['job']") == TSV([["api"], ["batch"], ["web"]])
        # The metric name via the `__name__` key.
        assert node.query("SELECT DISTINCT tags['__name__'] FROM prom_proj ORDER BY tags['__name__']") == TSV([["cpu"], ["direct"], ["mem"]])
        # A non-promoted tag (only in the Map).
        assert node.query("SELECT DISTINCT tags['host'] FROM prom_proj ORDER BY tags['host']") == TSV([["h1"], ["h2"], ["h3"]])
        # A tag that doesn't exist -> empty string.
        assert node.query("SELECT DISTINCT tags['missing'] FROM prom_proj") == TSV([[""]])
        # Several keys at once plus a WHERE on one of them.
        assert node.query(
            "SELECT tags['job'], tags['host'] FROM prom_proj WHERE tags['job'] = 'api'"
        ) == TSV([["api", "h1"]])
    finally:
        node.query("DROP TABLE IF EXISTS prom_proj SYNC")


def test_select_whole_tags_still_reconstructs_full_map():
    """When the query uses the whole `tags` Map (not just `tags['key']`), the full normalized reconstruction
    is used, so the reduced-tags optimization doesn't change the result."""
    node.query("DROP TABLE IF EXISTS prom_whole SYNC")
    node.query("CREATE TABLE prom_whole ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}")
    try:
        node.query(
            "INSERT INTO prom_whole (metric_name, tags, time_series) VALUES"
            " ('cpu', {'job': 'api', 'host': 'h1'}, [(toDateTime64(1000,3),1.0)])"
        )
        assert node.query("SELECT tags FROM prom_whole") == TSV([["{'__name__':'cpu','host':'h1','job':'api'}"]])
        # `mapKeys` forces the full Map too.
        assert node.query("SELECT mapKeys(tags) FROM prom_whole") == TSV([["['__name__','host','job']"]])
    finally:
        node.query("DROP TABLE IF EXISTS prom_whole SYNC")




def test_select_metrics_metadata_for_name_in_tags_map():
    """A series whose name lives in the inner tags Map under `__name__` (empty `metric_name` column) must still
    link to its family's metadata. The metadata FULL JOIN computes the family from the reconstructed name (with
    the `tags['__name__']` fallback), not the raw `metric_name` column — otherwise the family would be computed
    from an empty string, leaving the series with empty metadata and emitting a spurious metrics-only row."""
    # A series whose name ('cpu_usage') lives in the tags Map, with an empty metric_name column.
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES"
        " ('', {'__name__': 'cpu_usage', 'host': 'h1'})"
    )
    # Metadata for that family.
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('cpu_usage', 'gauge', 'percent', 'CPU usage')"
    )

    assert node.query(
        "SELECT metric_name, tags, metric_family, type, unit, help FROM prometheus"
    ) == TSV([["cpu_usage", "{'__name__':'cpu_usage','host':'h1'}", "cpu_usage", "gauge", "percent", "CPU usage"]])


def test_select_result_independent_of_null_settings():
    """The internal read joins the inner tables and relies on unmatched join rows / empty aggregate groups
    yielding empty strings (not NULL) to match the non-Nullable outer columns. The result must not depend on the
    caller's `join_use_nulls` / `aggregate_functions_null_for_empty` settings (which would otherwise inject NULLs
    into non-Nullable columns: silently wrong values, or an exception at the schema boundary)."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    # A series whose family has no metadata: its metadata columns come from an unmatched FULL JOIN row.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'host': 'h1'}, [(toDateTime64(2000, 3), 0.5)])"
    )

    metadata = TSV([
        ["cpu_usage", "", "", "", ""],
        ["http_requests_total", "http_requests", "counter", "requests", "Total HTTP requests"],
    ])
    virtual = TSV([
        ["prometheus", "cpu_usage", ""],
        ["prometheus", "http_requests_total", "counter"],
    ])
    for suffix in ["", " SETTINGS join_use_nulls = 1", " SETTINGS aggregate_functions_null_for_empty = 1"]:
        assert node.query(
            "SELECT metric_name, metric_family, type, unit, help FROM prometheus ORDER BY metric_name" + suffix
        ) == metadata
        # A constant virtual column forces a convert-to-header step that throws if a NULL reaches a non-Nullable column.
        assert node.query(
            "SELECT _table, metric_name, type FROM prometheus ORDER BY metric_name" + suffix
        ) == virtual


def test_select_nullable_tag_column():
    """A promoted tag column may be Nullable. A NULL value means 'absent', so the tag is read from the inner
    `tags` Map (reconstruction: the rebuilt `tags` / `tags['key']` stay non-Nullable), and a pushed-down WHERE on
    that column must be NULL-safe (`ifNull(col, '')`) so the row isn't wrongly pruned at the tags scan."""
    node.query("DROP TABLE IF EXISTS prom_nullable SYNC")
    node.query(
        "CREATE TABLE prom_nullable ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}"
        " TAGS INNER COLUMNS (job Nullable(String))"
    )
    try:
        # A series whose `job` lives in the tags Map while the promoted `job` column is NULL.
        node.query(
            "INSERT INTO FUNCTION timeSeriesTags(prom_nullable) (metric_name, tags, job) VALUES"
            " ('cpu', {'__name__': 'cpu', 'job': 'api'}, NULL)"
        )
        # Reconstruction: tags['job'] / tags come from the Map ('api'), not the NULL column, and stay non-Nullable.
        assert node.query("SELECT tags['job'] FROM prom_nullable") == TSV([["api"]])
        assert node.query("SELECT tags FROM prom_nullable") == TSV([["{'__name__':'cpu','job':'api'}"]])
        assert node.query("SELECT toTypeName(tags) FROM prom_nullable") == TSV([["Map(String, String)"]])
        # Push-down: each filter form must keep the NULL-column row (the value is in the Map).
        for condition in [
            "tags['job'] = 'api'",
            "startsWith(tags['job'], 'a')",
            "tags['job'] LIKE 'a%'",
            "match(tags['job'], '^api$')",
            "timeSeriesSelectorMatchTags('{job=\"api\"}', tags)",
        ]:
            assert node.query(f"SELECT metric_name FROM prom_nullable WHERE {condition}") == TSV([["cpu"]]), condition
    finally:
        node.query("DROP TABLE IF EXISTS prom_nullable SYNC")


def test_select_timestamp_filter_excludes_metadata_only_family():
    """A metric family with metadata but no in-window series must not leak through a `timestamp` filter: the
    representative timestamp of its unmatched metrics FULL-JOIN row is NULL, so a predicate that epoch 0 would
    satisfy (e.g. `timestamp <= C`, `timestamp != C`) correctly drops it."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(5000, 3), 1.0)], 'http_requests', 'counter')"
    )
    # A metadata-only family with no series at all.
    node.query("INSERT INTO prometheus (metric_family, type) VALUES ('memory_bytes', 'gauge')")

    # No series has a sample at or before 100s, so nothing should be returned -- in particular not the
    # metadata-only family (whose representative timestamp would otherwise default to epoch 0).
    assert node.query(
        "SELECT metric_name, metric_family FROM prometheus WHERE timestamp <= toDateTime64(100, 3)"
    ) == ""
    assert node.query(
        "SELECT metric_name, metric_family FROM prometheus WHERE timestamp != toDateTime64(5000, 3)"
    ) == ""
    # A matching window returns only the real series with its metadata.
    assert node.query(
        "SELECT metric_name, metric_family, type FROM prometheus WHERE timestamp >= toDateTime64(1000, 3)"
    ) == TSV([["http_requests_total", "http_requests", "counter"]])


def test_select_timestamp_filter_keeps_series_with_null_min_max_time():
    """min_time/max_time are NULL for a series whose samples were written directly into the samples table (which
    does not maintain them). The coarse tags-table range pruning must not drop such a series; the exact
    samples-table filter still applies."""
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES ('cpu', {'__name__': 'cpu'})"
    )
    node.query(
        "INSERT INTO FUNCTION timeSeriesData(prometheus) (id, timestamp, value)"
        " SELECT id, toDateTime64(2000, 3), 1.0 FROM timeSeriesTags(prometheus)"
    )
    # The sample at t=2000 satisfies these, so the series must be returned despite NULL min/max time.
    assert node.query("SELECT metric_name FROM prometheus WHERE timestamp >= toDateTime64(1500, 3)") == TSV([["cpu"]])
    assert node.query("SELECT metric_name FROM prometheus WHERE timestamp <= toDateTime64(2500, 3)") == TSV([["cpu"]])
    # ...and correctly excluded when the window genuinely does not contain the sample (exact samples filter).
    assert node.query("SELECT metric_name FROM prometheus WHERE timestamp >= toDateTime64(3000, 3)") == ""


def test_select_deduplicates_unmerged_tags_parts():
    """The tags inner table is AggregatingMergeTree; until a background merge runs, repeated inserts of the same
    series leave several unmerged parts with the same id. The read must return the series once (count() and the
    projection agree), not once per part."""
    # Two separate inserts of the SAME series -> two unmerged tags parts sharing one id.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)])"
    )
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('http_requests', {'job': 'api'}, [(toDateTime64(2000, 3), 2.0)])"
    )
    assert node.query("SELECT count() FROM prometheus") == "1\n"
    assert node.query("SELECT metric_name, tags FROM prometheus") == TSV([["http_requests", "{'__name__':'http_requests','job':'api'}"]])
    # The single series' samples from both parts are still gathered.
    assert node.query("SELECT metric_name, length(time_series) FROM prometheus") == TSV([["http_requests", "2"]])


def test_select_works_under_full_sorting_merge_join_algorithm():
    """The internal read uses SEMI / FULL joins, which the merge-join algorithm does not implement. A caller's
    `join_algorithm` must not break the read (`readImpl` pins `join_algorithm` on the internal query's context)."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter'),"
        " ('cpu_usage',           {'host': 'h1'}, [(toDateTime64(2000, 3), 0.5)], 'cpu_usage',     'gauge')"
    )
    for algo in ["full_sorting_merge", "direct,full_sorting_merge"]:
        # `time_series` exercises the samples SEMI JOIN; a metadata column exercises the metrics FULL JOIN.
        assert node.query(
            f"SELECT count() FROM (SELECT metric_name, time_series FROM prometheus) SETTINGS join_algorithm = '{algo}'"
        ) == "2\n"
        assert node.query(
            f"SELECT count() FROM (SELECT metric_name, type FROM prometheus) SETTINGS join_algorithm = '{algo}'"
        ) == "2\n"


def test_select_custom_samples_pk_splits_by_bucket():
    """When the inner `samples` table has a custom sorting key, the read groups by the sorting-key prefix up to
    `id` so the aggregation can run in sorting-key order. A per-sample key column (here `toStartOfHour(timestamp)`)
    precedes `id`, so a series splits into one row per bucket -- a faithful, re-insertable representation that the
    sink reassembles on INSERT. `count()` (tags-only) still counts one series."""
    node.query("DROP TABLE IF EXISTS t_custom SYNC")
    node.query(
        "CREATE TABLE t_custom ENGINE=TimeSeries"
        " DATA ENGINE=MergeTree() ORDER BY (toStartOfHour(timestamp), id)"
    )
    try:
        # Samples in two hour buckets: bucket 0 = {600s, 1000s}, bucket 1 = {4000s}.
        node.query(
            "INSERT INTO t_custom (metric_name, tags, time_series) VALUES"
            " ('cpu', {'job': 'api'}, [(toDateTime64(600, 3), 1.0), (toDateTime64(1000, 3), 2.0), (toDateTime64(4000, 3), 3.0)])"
        )
        # count() is tags-only -> one series (no split).
        assert node.query("SELECT count() FROM t_custom") == "1\n"
        # Projecting time_series groups samples per (hour, id): the series splits into two rows (lengths 2 and 1).
        assert node.query(
            "SELECT metric_name, length(time_series) FROM t_custom ORDER BY length(time_series)"
        ) == TSV([["cpu", "1"], ["cpu", "2"]])
        # The split rows re-insert into a default-PK table as one reassembled series (the sink merges by id).
        node.query(
            "INSERT INTO prometheus (metric_name, tags, time_series) SELECT metric_name, tags, time_series FROM t_custom"
        )
        assert node.query("SELECT metric_name, length(time_series) FROM prometheus") == TSV([["cpu", "3"]])
        # A timestamp filter still works: only bucket 1 (the sample at 4000s) is in window.
        assert node.query(
            "SELECT metric_name, length(time_series) FROM t_custom WHERE timestamp >= toDateTime64(3600, 3)"
        ) == TSV([["cpu", "1"]])
    finally:
        node.query("DROP TABLE IF EXISTS t_custom SYNC")
