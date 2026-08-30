import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry


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
    ) == TSV([["cpu_usage", "{'__name__':'cpu_usage','instance':'localhost:9090','job':'test'}"]])

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


def insert_time_series():
    """Helper for the SELECT tests: a series with its family's metadata, a series whose family has no
    metadata, and a metadata-only family with no series."""
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests_total', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0), (toDateTime64(2000, 3), 2.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'host': 'h1'}, [(toDateTime64(3000, 3), 0.5)])"
    )
    node.query(
        "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
        " ('memory_bytes', 'gauge', 'bytes', 'Memory usage')"
    )


ALL_COLUMNS_QUERY = (
    "SELECT metric_name, tags, time_series, metric_family, type, unit, help"
    " FROM prometheus ORDER BY metric_name, metric_family"
)

# Columns: metric_name, tags, time_series, metric_family, type, unit, help.
# A family emits one member name per suffix of its type; the counter family emits 'http_requests'
# and 'http_requests_total', and the bare member has no series, so it shows up as an unmatched row,
# same as the metadata-only family.
ALL_COLUMNS_EXPECTED = TSV([
    ["",                    "{}",                                             "[]",                                                             "http_requests", "counter", "requests", "Total HTTP requests"],
    ["",                    "{}",                                             "[]",                                                             "memory_bytes",  "gauge",   "bytes",    "Memory usage"],
    ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}",           "[('1970-01-01 00:50:00.000',0.5)]",                              "",              "",        "",         ""],
    ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "[('1970-01-01 00:16:40.000',1),('1970-01-01 00:33:20.000',2)]", "http_requests", "counter", "requests", "Total HTTP requests"],
])


def test_select_all_columns():
    """Reads all three target tables at once (the aggregated samples SEMI-joined to the "tags" table, the
    "metrics" table FULL-joined on top): a series with metadata, a series whose family has no metadata
    (kept, with empty metadata columns), and a metadata-only family (kept, with empty series columns)."""
    insert_time_series()

    assert node.query(ALL_COLUMNS_QUERY) == ALL_COLUMNS_EXPECTED


def test_select_time_series():
    """Reads only the "samples" table: one row per series carrying its grouped (timestamp, value) tuples
    (the metadata-only family has no samples and no series, so it isn't visible here)."""
    insert_time_series()

    assert node.query(
        "SELECT time_series FROM prometheus ORDER BY length(time_series)"
    ) == TSV([
        ["[('1970-01-01 00:50:00.000',0.5)]"],
        ["[('1970-01-01 00:16:40.000',1),('1970-01-01 00:33:20.000',2)]"],
    ])


def test_select_metric_name_and_tags():
    """Reads only the "tags" table: the `metric_name` and `tags` outer columns are reconstructed from it -
    together and each on its own; `count()` returns the number of series. A row written directly into the
    inner table may keep the metric name only in the tags Map under `__name__`: the reconstructed `tags` Map
    merges it in, while the `metric_name` column reads the inner column as is (empty)."""
    insert_time_series()
    # A row with the metric name only in the tags Map (empty `metric_name` column).
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags) VALUES"
        " ('', {'__name__': 'bar', 'x': '1'})"
    )

    assert node.query("SELECT count() FROM prometheus") == "3\n"

    assert node.query(
        "SELECT metric_name, tags FROM prometheus ORDER BY metric_name"
    ) == TSV([
        ["",                    "{'__name__':'bar','x':'1'}"],
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}"],
    ])
    assert node.query("SELECT metric_name FROM prometheus ORDER BY metric_name") == TSV([
        [""], ["cpu_usage"], ["http_requests_total"],
    ])
    assert node.query("SELECT tags FROM prometheus ORDER BY tags") == TSV([
        ["{'__name__':'bar','x':'1'}"],
        ["{'__name__':'cpu_usage','host':'h1'}"],
        ["{'__name__':'http_requests_total','job':'api'}"],
    ])


def test_select_metric_families():
    """Reads only the "metrics" table: one row per metric family with its metadata, independent of how many
    time series belong to it; duplicated metadata rows collapse into one."""
    insert_time_series()
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


def test_select_time_series_and_metric_name_and_tags():
    """Reads the "samples" and "tags" tables: the tags attach to the aggregated samples, one output row
    per series."""
    insert_time_series()

    assert node.query(
        "SELECT metric_name, tags, length(time_series) AS n FROM prometheus ORDER BY metric_name"
    ) == TSV([
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}",           "1"],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}", "2"],
    ])


def test_select_metric_name_and_tags_and_metric_family():
    """Reads the "tags" and "metrics" tables. A series is matched to the family whose type emits its name:
    metadata is shared by all series of the family; duplicated family metadata does not multiply the series;
    a series whose family has no metadata keeps empty metadata columns; an orphan metric family with no series
    is still returned (with an empty metric_name). The match checks the type: a gauge named `queue_count` links
    to its own family `queue_count` (it is not misfiled under a suffix-stripped family), while a histogram's
    `_bucket` series links to its `http_request_duration` family."""
    insert_time_series()
    # A second series of the same family, and the family's metadata inserted once more
    # (must not multiply the series).
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type) VALUES"
        " ('http_requests_total', {'job': 'web'}, [(toDateTime64(2000, 3), 2.0)], 'http_requests', 'counter')"
    )
    # A gauge whose name genuinely ends in '_count' and a real histogram's '_bucket' series.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type) VALUES"
        " ('queue_count', {'q': 'jobs'}, [(toDateTime64(4000, 3), 7.0)], 'queue_count', 'gauge'),"
        " ('http_request_duration_bucket', {'le': '0.5'}, [(toDateTime64(5000, 3), 3.0)], 'http_request_duration', 'histogram')"
    )
    # A counter whose family name already includes '_total' and equals the series name.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type) VALUES"
        " ('errors_total', {'app': 'web'}, [(toDateTime64(6000, 3), 1.0)], 'errors_total', 'counter')"
    )

    # Each series links to the metadata of a matching type.
    assert node.query(
        "SELECT metric_name, tags, metric_family, type FROM prometheus"
        " WHERE metric_name != '' ORDER BY metric_name, tags"
    ) == TSV([
        ["cpu_usage",                    "{'__name__':'cpu_usage','host':'h1'}",                    "",                      ""],
        ["errors_total",                 "{'__name__':'errors_total','app':'web'}",                 "errors_total",          "counter"],
        ["http_request_duration_bucket", "{'__name__':'http_request_duration_bucket','le':'0.5'}", "http_request_duration", "histogram"],
        ["http_requests_total",          "{'__name__':'http_requests_total','job':'api'}",          "http_requests",         "counter"],
        ["http_requests_total",          "{'__name__':'http_requests_total','job':'web'}",          "http_requests",         "counter"],
        ["queue_count",                  "{'__name__':'queue_count','q':'jobs'}",                   "queue_count",           "gauge"],
    ])

    # Unmatched rows keep an empty metric_name: the orphan family, the histogram's absent
    # '_count'/'_sum' members, and the counters' absent members ('errors_total_total' and
    # the bare 'http_requests').
    assert node.query(
        "SELECT metric_family, type FROM prometheus WHERE metric_name = '' ORDER BY metric_family"
    ) == TSV([
        ["errors_total",          "counter"],
        ["http_request_duration", "histogram"],
        ["http_request_duration", "histogram"],
        ["http_requests",         "counter"],
        ["memory_bytes",          "gauge"],
    ])


def test_select_time_series_and_metric_family():
    """Reads "samples" and "metrics" columns only; the "tags" table is still read internally as the bridge
    between them (samples are joined by id, metrics by the metric name)."""
    insert_time_series()

    assert node.query(
        "SELECT length(time_series) AS n, type, unit FROM prometheus ORDER BY n, type"
    ) == TSV([
        ["0", "counter", "requests"],
        ["0", "gauge",   "bytes"],
        ["1", "",        ""],
        ["2", "counter", "requests"],
    ])


def check_inner_columns_read(query, expected_column, forbidden_column):
    """Runs the query and checks via system.query_log that it read `expected_column` of an inner target table
    and did not read `forbidden_column`."""
    query_id = f"ts-columns-{uuid.uuid4()}"
    node.query(query, query_id=query_id)
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT arrayExists(c -> endsWith(c, '.{expected_column}') AND position(c, 'inner') != 0, columns),"
        f" arrayExists(c -> endsWith(c, '.{forbidden_column}') AND position(c, 'inner') != 0, columns)"
        f" FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\t0",
    )


def test_select_tags_with_separate_column():
    """Tags with dedicated columns (the `tags_to_columns` setting): the sink stores them in their
    columns (`job`, and a Nullable `instance`) in addition to the tags Map, while a row written
    directly into the inner table can keep such a tag only in its column. Reading `tags` returns the
    full Map either way; reading `tags['job']` returns the value from the column without reading the (heavy)
    inner `tags` Map column at all; `tags['__name__']` likewise reads only the `metric_name` column."""
    # Recreate the table with dedicated columns for the tags `job` and `instance`.
    node.query("DROP TABLE prometheus SYNC")
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job', 'instance': 'instance'}"
        " TAGS INNER COLUMNS (instance Nullable(String))"
    )
    insert_time_series()
    # A series using both tags with dedicated columns and a tag without one.
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('mem_free', {'job': 'web', 'instance': 'host1', 'region': 'eu'}, [(toDateTime64(4000, 3), 3.0)])"
    )
    # A row written directly into the inner table: `job` is only in its column, not in the Map.
    node.query(
        "INSERT INTO FUNCTION timeSeriesTags(prometheus) (metric_name, tags, job) VALUES"
        " ('disk_usage', {'fs': 'ext4'}, 'batch')"
    )

    # The tags with dedicated columns are stored in those columns (the inner tags Map keeps a copy too).
    assert node.query(
        "SELECT metric_name, job, ifNull(instance, ''), tags FROM timeSeriesTags(prometheus) ORDER BY metric_name"
    ) == TSV([
        ["cpu_usage",           "",      "",      "{'__name__':'cpu_usage','host':'h1'}"],
        ["disk_usage",          "batch", "",      "{'fs':'ext4'}"],
        ["http_requests_total", "api",   "",      "{'__name__':'http_requests_total','job':'api'}"],
        ["mem_free",            "web",   "host1", "{'__name__':'mem_free','instance':'host1','job':'web','region':'eu'}"],
    ])

    # Reading the whole `tags` returns the full Map (the tags stored only in their dedicated columns
    # are merged in).
    assert node.query("SELECT metric_name, tags FROM prometheus ORDER BY metric_name") == TSV([
        ["cpu_usage",           "{'__name__':'cpu_usage','host':'h1'}"],
        ["disk_usage",          "{'__name__':'disk_usage','fs':'ext4','job':'batch'}"],
        ["http_requests_total", "{'__name__':'http_requests_total','job':'api'}"],
        ["mem_free",            "{'__name__':'mem_free','instance':'host1','job':'web','region':'eu'}"],
    ])

    # Reading `tags['<key>']` returns the tag values (an absent tag is an empty string).
    assert node.query(
        "SELECT metric_name, tags['job'], tags['instance'], tags['region'] FROM prometheus ORDER BY metric_name"
    ) == TSV([
        ["cpu_usage",           "",      "",      ""],
        ["disk_usage",          "batch", "",      ""],
        ["http_requests_total", "api",   "",      ""],
        ["mem_free",            "web",   "host1", "eu"],
    ])

    # `tags['job']` and `tags['instance']` are served from their columns without reading the inner
    # `tags` Map column, and `tags['__name__']` from the `metric_name` column.
    check_inner_columns_read("SELECT tags['job'] FROM prometheus FORMAT Null", "job", "tags")
    check_inner_columns_read("SELECT tags['instance'] FROM prometheus FORMAT Null", "instance", "tags")
    check_inner_columns_read("SELECT tags['__name__'] FROM prometheus FORMAT Null", "metric_name", "tags")


def test_select_with_row_policy():
    """A row policy (and likewise the `additional_table_filters` setting) filters the rows this storage
    returns, but its expression is not part of the query. A filter over the `tags` Map must keep filtering
    even when the query itself reads only `tags['<key>']`: the tags the filter checks are added to the
    reduced tags Map (served from their dedicated columns where they have one), and a filter using `tags`
    in a way that cannot be reduced disables the reduction (the full Map is built). The table has a
    dedicated column for the tag `job` so the scenarios can verify which inner columns are read."""
    # Recreate the table with a dedicated column for the tag `job`.
    node.query("DROP TABLE prometheus SYNC")
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS tags_to_columns={'job': 'job'}"
    )
    insert_time_series()
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('mem_free', {'job': 'web', 'region': 'eu'}, [(toDateTime64(4000, 3), 3.0)])"
    )

    # A policy checking the tag with a dedicated column: it hides 'mem_free' (whose 'job' tag is 'web')
    # while the read serves the tag from its column without reading the inner `tags` Map column at all.
    node.query(
        "CREATE ROW POLICY policy1 ON prometheus FOR SELECT USING tags['job'] != 'web' TO default"
    )
    try:
        assert node.query("SELECT tags['job'] FROM prometheus ORDER BY 1") == TSV([[""], ["api"]])
        check_inner_columns_read("SELECT tags['job'] FROM prometheus FORMAT Null", "job", "tags")
    finally:
        node.query("DROP ROW POLICY policy1 ON prometheus")

    # The policy checks tags['host'] (a tag stored in the Map), so that key is added to the reduced tags
    # Map. The query reads only tags['job'], but the policy must still hide the 'cpu_usage' series (whose
    # 'host' tag is 'h1').
    node.query("CREATE ROW POLICY policy2 ON prometheus FOR SELECT USING tags['host'] != 'h1' TO default")
    try:
        assert node.query("SELECT metric_name FROM prometheus ORDER BY metric_name") == TSV(
            [["http_requests_total"], ["mem_free"]]
        )
        assert node.query("SELECT tags['job'] FROM prometheus ORDER BY 1") == TSV([["api"], ["web"]])
    finally:
        node.query("DROP ROW POLICY policy2 ON prometheus")

    # This policy uses the whole `tags` Map, so the reduction is disabled and the full Map is built.
    node.query(
        "CREATE ROW POLICY policy3 ON prometheus FOR SELECT USING NOT mapContains(tags, 'host') TO default"
    )
    try:
        assert node.query("SELECT tags['job'] FROM prometheus ORDER BY 1") == TSV([["api"], ["web"]])
    finally:
        node.query("DROP ROW POLICY policy3 ON prometheus")

    # The `additional_table_filters` setting must keep filtering the same way as a row policy.
    filter_settings = " SETTINGS additional_table_filters = {'prometheus': 'tags[''host''] != ''h1'''}"
    assert node.query("SELECT metric_name FROM prometheus ORDER BY metric_name" + filter_settings) == TSV(
        [["http_requests_total"], ["mem_free"]]
    )
    assert node.query("SELECT tags['job'] FROM prometheus ORDER BY 1" + filter_settings) == TSV(
        [["api"], ["web"]]
    )


def test_select_final():
    """The tags inner table is AggregatingMergeTree; until its parts merge, repeated inserts of one series
    leave duplicate rows. Without FINAL the read returns them as is (cheaper); with FINAL the series is
    returned exactly once. After the parts are merged both reads agree."""
    node.query("SYSTEM STOP MERGES")
    try:
        # Two separate inserts of the SAME series -> two unmerged tags parts sharing one id.
        node.query(
            "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
            " ('http_requests', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0)])"
        )
        node.query(
            "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
            " ('http_requests', {'job': 'api'}, [(toDateTime64(2000, 3), 2.0)])"
        )

        # Unmerged: without FINAL the series is returned once per part, with FINAL exactly once.
        assert node.query("SELECT count() FROM prometheus") == "2\n"
        assert node.query("SELECT count() FROM prometheus FINAL") == "1\n"
        assert node.query("SELECT metric_name, tags FROM prometheus FINAL") == TSV(
            [["http_requests", "{'__name__':'http_requests','job':'api'}"]]
        )
        # The single series' samples from both parts are still gathered into one array.
        assert node.query("SELECT metric_name, length(time_series) FROM prometheus FINAL") == TSV(
            [["http_requests", "2"]]
        )
    finally:
        node.query("SYSTEM START MERGES")

    # Merged: both reads agree.
    node.query("OPTIMIZE TABLE prometheus FINAL")
    assert node.query("SELECT count() FROM prometheus") == "1\n"
    assert node.query("SELECT count() FROM prometheus FINAL") == "1\n"


def test_select_pin_settings():
    """The internal read must not depend on the caller's settings: it pins `join_use_nulls`,
    `aggregate_functions_null_for_empty`, `join_algorithm` and `optimize_aggregation_in_order` on its
    own context. Selecting all the columns with all of those settings set to wrong values must return
    exactly the same result."""
    insert_time_series()

    wrong_settings = (
        " SETTINGS join_use_nulls = 1,"
        " aggregate_functions_null_for_empty = 1,"
        " join_algorithm = 'full_sorting_merge',"
        " optimize_aggregation_in_order = 0,"
        " allow_aggregate_partitions_independently = 0,"
        " force_aggregate_partitions_independently = 0"
    )

    assert node.query(ALL_COLUMNS_QUERY + wrong_settings) == ALL_COLUMNS_EXPECTED

    # A constant virtual column forces a convert-to-header step that throws if a NULL reaches
    # a non-Nullable column.
    assert node.query(
        "SELECT _table, metric_name, type FROM prometheus ORDER BY metric_name, type" + wrong_settings
    ) == TSV([
        ["prometheus", "",                    "counter"],
        ["prometheus", "",                    "gauge"],
        ["prometheus", "cpu_usage",           ""],
        ["prometheus", "http_requests_total", "counter"],
    ])
