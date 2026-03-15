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


# Checks that the `id` column type can be changed to `UInt64`.
def test_64bit_id():
    node.query("CREATE TABLE prometheus (id UInt64) ENGINE=TimeSeries")
    check()
    assert re.search(r"\bid\s+UInt64", node.query("DESCRIBE timeSeriesTags(prometheus)"))


# Checks that a custom hash function can be used to generate time series identifiers.
def test_custom_id_algorithm():
    node.query(
        "CREATE TABLE prometheus (id FixedString(16) DEFAULT murmurHash3_128(metric_name, all_tags)) ENGINE=TimeSeries"
    )
    check()
    assert re.search(r"\bid\s+FixedString\(16\)", node.query("DESCRIBE timeSeriesTags(prometheus)"))


# Checks that timestamps can be stored with microsecond precision (`DateTime64(6)`).
def test_microsecond_precision():
    node.query("CREATE TABLE prometheus (timestamp DateTime64(6)) ENGINE=TimeSeries")
    check(eps=1e-9)  # Here eps > 0 because otherwise the check will fail because of different precisions.
    assert re.search(r"\btimestamp\s+DateTime64\(6\)", node.query("DESCRIBE timeSeriesData(prometheus)"))


# Checks that scalar values can be stored as `Float32` instead of the default `Float64`.
def test_float32_scalar():
    node.query("CREATE TABLE prometheus (value Float32) ENGINE=TimeSeries")
    check()
    assert re.search(r"\bvalue\s+Float32", node.query("DESCRIBE timeSeriesData(prometheus)"))


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
        "DATA ENGINE=MergeTree ORDER BY (id, timestamp) "
        "TAGS ENGINE=AggregatingMergeTree ORDER BY (metric_name, id) "
        "METRICS ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    check()


# Checks that a TimeSeries table can be backed by pre-existing external tables
# instead of auto-created inner tables.
def test_external_tables():
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

    # FIXME: The table structure should be:
    # "CREATE TABLE mymetrics (metric_family_name String, type LowCardinality(String), unit LowCardinality(String), help String)"
    # Renamed it because of the bug and potential type mismatch.
    node.query(
        "CREATE TABLE mymetrics (metric_family_name String, type String, unit String, help String) "
        "ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "DATA mydata TAGS mytags METRICS mymetrics"
    )
    check()
