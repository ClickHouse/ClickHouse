import pytest
import re

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, tsv_close_to
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_reader=True,
    with_prometheus_receiver=True,
)

# Loads time series from preset.
preset = load_preset("demo.promlabs.com-30s.zip")
timestamp = 1753199684.626


def send_preset_to_prometheus_receiver():
    send_protobuf_to_remote_write(
        cluster.prometheus_ip["receiver"],
        cluster.prometheus_port["receiver"],
        "api/v1/write",
        preset,
    )


def send_preset_to_clickhouse():
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", preset)


# Queries to check.
test_queries = [
    (
        'up{instance=~"demo-service-0:.*"}',
        '{"resultType": "vector", "result": [{"metric": {"__name__": "up", "instance": "demo-service-0:10000", "job": "demo"}, "value": [1753199684.626, "1"]}]}',
        [
            [
                "[('__name__','up'),('instance','demo-service-0:10000'),('job','demo')]",
                "2025-07-22 15:54:44.626",
                "1",
            ]
        ],
    ),
    (
        'irate(prometheus_http_requests_total{code="200",handler="/api/v1/query"}[30s])',
        '{"resultType": "vector", "result": [{"metric": {"code": "200", "handler": "/api/v1/query", "instance": "prometheus:9090", "job": "prometheus"}, "value": [1753199684.626, "0.2"]}]}',
        [
            [
                "[('code','200'),('handler','/api/v1/query'),('instance','prometheus:9090'),('job','prometheus')]",
                "2025-07-22 15:54:44.626",
                "0.2",
            ]
        ],
    ),
]


# Executes the test queries in the "prometheus_receiver" service and check the results.
# We send data to the "prometheus_receiver" directly via RemoteWrite protocol.
def check_queries_in_prometheus_receiver(eps=0):
    for query, result, _ in test_queries:
        assert tsv_close_to(
            execute_query_via_http_api(
                cluster.prometheus_ip["receiver"],
                cluster.prometheus_port["receiver"],
                "/api/v1/query",
                query,
                timestamp,
            )
            , result
            , eps=eps
        )


# Executes the test queries in the "prometheus_reader" service and check the results.
# We send data to ClickHouse via RemoteWrite protocol and
# then "prometheus_reader" reads data from ClickHouse via RemoteRead protocol.
def check_queries_in_prometheus_reader(eps=0):
    for query, result, _ in test_queries:
        assert tsv_close_to(
            execute_query_via_http_api(
                cluster.prometheus_ip["reader"],
                cluster.prometheus_port["reader"],
                "/api/v1/query",
                query,
                timestamp,
            )
            , result
            , eps=eps
        )


# Executes the test queries in ClickHouse and test the results.
def check_queries_in_clickhouse(eps=0):
    for query, _, chresult in test_queries:
        assert tsv_close_to(
            node.query(f"SELECT * FROM prometheusQuery(prometheus, '{query}', {timestamp})"),
            chresult,
            eps=eps,
        )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        send_preset_to_prometheus_receiver()
        check_queries_in_prometheus_receiver()
        yield cluster
    finally:
        cluster.shutdown()


# Sends presets to clickhouse and execute the test queries.
def check(eps=0):
    send_preset_to_clickhouse()
    check_queries_in_prometheus_reader(eps=eps)
    check_queries_in_clickhouse(eps=eps)

# Drops TimeSeries table
def drop_prometheus_table():
    node.query("DROP TABLE prometheus SYNC")


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS prometheus SYNC")
        node.query("DROP TABLE IF EXISTS original SYNC")
        node.query("DROP TABLE IF EXISTS mytable SYNC")
        node.query("DROP TABLE IF EXISTS mysamples SYNC")
        node.query("DROP TABLE IF EXISTS mydata SYNC")
        node.query("DROP TABLE IF EXISTS mytags SYNC")
        node.query("DROP TABLE IF EXISTS mymetrics SYNC")


# Checks that a TimeSeries table works with all default settings.
def test_default():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    check()


# Checks that specific tag labels can be extracted into dedicated columns
# instead of being stored in the generic `tags` map.
def test_tags_to_columns():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'}"
    )

    check()

    describe = node.query("DESCRIBE timeSeriesTags(prometheus)")
    assert re.search(r"\bjob\s+String", describe)
    assert re.search(r"\binstance\s+String", describe)

    assert node.query("SELECT job, instance FROM timeSeriesTags(prometheus) WHERE metric_name = 'up' AND instance = 'demo-service-0:10000'") == TSV([["demo", "demo-service-0:10000"]])


# Checks that the `id` column type can be changed to `UInt64` via either
# `TAGS INNER COLUMNS` or `SAMPLES INNER COLUMNS`.
def test_64bit_id():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries TAGS INNER COLUMNS (id UInt64)")
    check()
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)TAGS INNER COLUMNS.*`id` UInt64", create_query)
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`id` UInt64", create_query)
    assert re.search(r"\bid\s+UInt64", node.query("DESCRIBE timeSeriesTags(prometheus)"))
    assert re.search(r"\bid\s+UInt64", node.query("DESCRIBE timeSeriesSamples(prometheus)"))

    drop_prometheus_table()

    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SAMPLES INNER COLUMNS (id UInt64)")
    check()
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)TAGS INNER COLUMNS.*`id` UInt64", create_query)
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`id` UInt64", create_query)
    assert re.search(r"\bid\s+UInt64", node.query("DESCRIBE timeSeriesTags(prometheus)"))
    assert re.search(r"\bid\s+UInt64", node.query("DESCRIBE timeSeriesSamples(prometheus)"))


# Checks that a custom hash function can be used to generate time series identifiers.
def test_custom_id_algorithm():
    # Case 1: customize via `TAGS INNER COLUMNS (id ... DEFAULT ...)`.
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "TAGS INNER COLUMNS (id FixedString(16) DEFAULT murmurHash3_128(metric_name, all_tags))"
    )
    check()
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`id` FixedString\(16\)", create_query)
    assert re.search(r"(?s)TAGS INNER COLUMNS.*`id` FixedString\(16\) DEFAULT murmurHash3_128\(metric_name, all_tags\)", create_query)
    assert re.search(r"\bid\s+FixedString\(16\)", node.query("DESCRIBE timeSeriesTags(prometheus)"))
    tags_table = node.query("SELECT _table FROM timeSeriesTags(prometheus) LIMIT 1").strip()
    assert node.query(
        f"SELECT type, default_expression FROM system.columns "
        f"WHERE database = currentDatabase() AND table = '{tags_table}' AND name = 'id'"
    ) == TSV([["FixedString(16)", "murmurHash3_128(metric_name, all_tags)"]])

    drop_prometheus_table()

    # Case 2: customize via the `id_generator` setting.
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "SETTINGS id_generator = 'murmurHash3_128(metric_name, all_tags)' "
        "TAGS INNER COLUMNS (id FixedString(16))"
    )
    check()
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"\bid_generator\s*=.*murmurHash3_128\(metric_name, all_tags\)", create_query)
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`id` FixedString\(16\)", create_query)
    tags_table = node.query("SELECT _table FROM timeSeriesTags(prometheus) LIMIT 1").strip()
    assert node.query(
        f"SELECT type, default_expression FROM system.columns "
        f"WHERE database = currentDatabase() AND table = '{tags_table}' AND name = 'id'"
    ) == TSV([["FixedString(16)", ""]])


# Checks that timestamps can be stored with microsecond precision (`DateTime64(6)`).
def test_microsecond_precision():
    node.query("CREATE TABLE prometheus (time_series Array(Tuple(DateTime64(6), Float64))) ENGINE=TimeSeries")
    check(eps=1e-9) # Here eps > 0 because otherwise the check will fail because of different precisions.
    assert node.query("SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus' AND name = 'time_series'") == TSV([["Array(Tuple(DateTime64(6), Float64))"]])
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`timestamp` DateTime64\(6\)", create_query)
    assert re.search(r"\btimestamp\s+DateTime64\(6\)", node.query("DESCRIBE timeSeriesSamples(prometheus)"))

    drop_prometheus_table()

    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(6))")
    check(eps=1e-9)
    assert node.query("SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus' AND name = 'time_series'") == TSV([["Array(Tuple(DateTime64(6), Float64))"]])
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`timestamp` DateTime64\(6\)", create_query)
    assert re.search(r"\btimestamp\s+DateTime64\(6\)", node.query("DESCRIBE timeSeriesSamples(prometheus)"))


# Checks that scalar values can be stored as `Float32` instead of the default `Float64`.
def test_float32_scalar():
    node.query("CREATE TABLE prometheus (time_series Array(Tuple(DateTime64(3), Float32))) ENGINE=TimeSeries")
    check()
    assert node.query("SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus' AND name = 'time_series'") == TSV([["Array(Tuple(DateTime64(3), Float32))"]])
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`value` Float32", create_query)
    assert re.search(r"\bvalue\s+Float32", node.query("DESCRIBE timeSeriesSamples(prometheus)"))

    drop_prometheus_table()

    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SAMPLES INNER COLUMNS (value Float32)")
    check()
    assert node.query("SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus' AND name = 'time_series'") == TSV([["Array(Tuple(DateTime64(3), Float32))"]])
    create_query = node.query("SHOW CREATE TABLE prometheus")
    assert re.search(r"(?s)SAMPLES INNER COLUMNS.*`value` Float32", create_query)
    assert re.search(r"\bvalue\s+Float32", node.query("DESCRIBE timeSeriesSamples(prometheus)"))


# Checks that custom compression codecs can be applied to the `id`, `timestamp`, and `value` columns.
def test_custom_codecs():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "TAGS INNER COLUMNS (id UUID CODEC(ZSTD)) "
        "SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta), value Float64 CODEC(Gorilla))"
    )
    check()

    tags_table = node.query("SELECT _table FROM timeSeriesTags(prometheus) LIMIT 1").strip()
    samples_table = node.query("SELECT _table FROM timeSeriesSamples(prometheus) LIMIT 1").strip()

    assert node.query(
        f"SELECT type, default_expression, compression_codec FROM system.columns "
        f"WHERE database = currentDatabase() AND table = '{tags_table}' AND name = 'id'"
    ) == TSV([["UUID", "reinterpretAsUUID(sipHash128(metric_name, all_tags))", "CODEC(ZSTD(1))"]])

    assert node.query(
        f"SELECT type, compression_codec FROM system.columns "
        f"WHERE database = currentDatabase() AND table = '{samples_table}' AND name = 'timestamp'"
    ) == TSV([["DateTime64(3)", "CODEC(DoubleDelta)"]])

    assert node.query(
        f"SELECT type, compression_codec FROM system.columns "
        f"WHERE database = currentDatabase() AND table = '{samples_table}' AND name = 'value'"
    ) == TSV([["Float64", "CODEC(Gorilla(8))"]])


# Checks that a TimeSeries table can be created as a copy of another TimeSeries table,
# inheriting its settings and structure.
def test_create_as_table():
    node.query("CREATE TABLE original ENGINE=TimeSeries")
    node.query("CREATE TABLE prometheus AS original")
    check()


# Checks that the storage engines of the inner tables can be customized.
def test_inner_engines():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "SAMPLES ENGINE=MergeTree ORDER BY (id, timestamp) "
        "TAGS ENGINE=AggregatingMergeTree ORDER BY (metric_name, id) "
        "METRICS ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    check()


# Checks that a TimeSeries table can be used to access pre-existing external tables
# instead of its own inner tables.
def test_external_tables():
    node.query(
        "CREATE TABLE mysamples (id UUID, timestamp DateTime64(3), value Float64) "
        "ENGINE=MergeTree ORDER BY (id, timestamp)"
    )

    node.query(
        "CREATE TABLE mytags ("
        "id UUID, "
        "metric_name LowCardinality(String), "
        "tags Map(LowCardinality(String), String), "
        "min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
        "max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))) "
        "ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
    )

    node.query(
        "CREATE TABLE mymetrics (metric_family_name String, type LowCardinality(String), unit LowCardinality(String), help String) "
        "ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "SAMPLES mysamples TAGS mytags METRICS mymetrics"
    )
    check()


# Checks that the `DATA` keyword works as an alias for `SAMPLES`
# both with inline engine definitions and with external tables.
def test_data_keyword():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "DATA ENGINE=MergeTree ORDER BY (id, timestamp) "
        "TAGS ENGINE=AggregatingMergeTree ORDER BY (metric_name, id) "
        "METRICS ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    check()

    drop_prometheus_table()

    node.query(
        "CREATE TABLE mydata (id UUID, timestamp DateTime64(3), value Float64) "
        "ENGINE=MergeTree ORDER BY (id, timestamp)"
    )
    node.query(
        "CREATE TABLE mytags ("
        "id UUID, "
        "metric_name LowCardinality(String), "
        "tags Map(LowCardinality(String), String), "
        "min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
        "max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))) "
        "ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
    )
    node.query(
        "CREATE TABLE mymetrics (metric_family_name String, type String, unit String, help String) "
        "ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "DATA mydata TAGS mytags METRICS mymetrics"
    )
    check()

    assert node.query("DESCRIBE timeSeriesData(prometheus)") == node.query("DESCRIBE timeSeriesSamples(prometheus)")


# Checks that ALTER TABLE works and can modify settings
# `id_generator` and `filter_by_min_time_and_max_time`.
def test_alter_modify_settings():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    # `id_generator` only affects INSERT-time id computation, so it can be altered.
    node.query("ALTER TABLE prometheus MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)'")
    assert re.search(
        r"\bid_generator\s*=.*sipHash64\(metric_name, all_tags\)",
        node.query("SHOW CREATE TABLE prometheus"),
    )
    node.query("ALTER TABLE prometheus RESET SETTING id_generator")

    # `filter_by_min_time_and_max_time` is a pure query-time setting, so it can be altered.
    node.query("ALTER TABLE prometheus MODIFY SETTING filter_by_min_time_and_max_time = 0")
    assert re.search(
        r"\bfilter_by_min_time_and_max_time\s*=\s*false",
        node.query("SHOW CREATE TABLE prometheus"),
    )
    node.query("ALTER TABLE prometheus RESET SETTING filter_by_min_time_and_max_time")

    # Settings which can't be altered because they affect inner-table schemas.
    bound_settings = [
        "tags_to_columns = {'job': 'job'}",
        "use_all_tags_column_to_generate_id = 0",
        "store_min_time_and_max_time = 0",
        "aggregate_min_time_and_max_time = 0",
    ]
    for change in bound_settings:
        assert "NOT_IMPLEMENTED" in node.query_and_get_error(
            f"ALTER TABLE prometheus MODIFY SETTING {change}"
        )
