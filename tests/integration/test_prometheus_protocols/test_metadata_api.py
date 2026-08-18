import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    receive_protobuf_from_remote_read,
    remote_pb2,
    send_protobuf_to_remote_write,
    types_pb2,
)


cluster = ClickHouseCluster(__file__)
MAIN_HTTP_PORT = 8123

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_metadata.xml",
        "configs/config.d/query_log.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/prometheus_metadata_users.xml",
    ],
)


def get_metadata(params=None, headers=None, auth=None, path="/api/v1/metadata"):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        params=params or {},
        headers=headers or {},
        auth=auth,
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "success", result
    return result["data"]


def get_external_metadata(params=None, auth=None):
    return get_metadata(params=params, auth=auth, path="/external/api/v1/metadata")


def get_nullable_metadata(params=None, auth=None):
    return get_metadata(params=params, auth=auth, path="/nullable/api/v1/metadata")


def get_wrapped_metadata(params=None, headers=None, auth=None):
    return get_metadata(
        params=params,
        headers=headers,
        auth=auth,
        path="/wrapped/api/v1/metadata",
    )


def get_distributed_plain_metadata(params=None, headers=None, auth=None):
    return get_metadata(
        params=params,
        headers=headers,
        auth=auth,
        path="/distributed-plain/api/v1/metadata",
    )


def get_quota_metadata(params=None, auth=None):
    return get_metadata(params=params, auth=auth, path="/quota/api/v1/metadata")


def get_high_card_metadata(params=None, headers=None, auth=None):
    return get_metadata(
        params=params,
        headers=headers,
        auth=auth,
        path="/high-card/api/v1/metadata",
    )


def get_pruning_metadata(params=None, headers=None, auth=None):
    return get_metadata(
        params=params,
        headers=headers,
        auth=auth,
        path="/pruning/api/v1/metadata",
    )


def execute_sql(query, auth, params=None):
    return requests.post(
        f"http://{node.ip_address}:{MAIN_HTTP_PORT}",
        data=query,
        auth=auth,
        params=params or {},
    )


def assert_metadata_subset(data, metric, expected_entries, limit):
    entries = data[metric]
    assert 0 < len(entries) <= limit
    assert (
        len({(entry["type"], entry["help"], entry["unit"]) for entry in entries})
        == len(entries)
    )
    assert all(entry in expected_entries for entry in entries)


def optimize_table_final(table_name):
    node.query("SYSTEM START MERGES")
    try:
        node.query(f"OPTIMIZE TABLE {table_name} FINAL")
    finally:
        node.query("SYSTEM STOP MERGES")


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query("SYSTEM STOP MERGES")
        node.query(
            "INSERT INTO prometheus "
            "(metric_name, tags, time_series, metric_family, type, unit, help) VALUES "
            "('http_requests_total', {'instance':'a'}, [(toDateTime64(1000, 3), 1)], "
            " 'http_requests_total', 'counter', '', 'Number of HTTP requests'), "
            "('http_requests_total', {'instance':'b'}, [(toDateTime64(1000, 3), 2)], "
            " 'http_requests_total', 'counter', '', 'Number of HTTP requests'), "
            "('cpu_usage', {'instance':'a'}, [(toDateTime64(1000, 3), 0.5)], "
            " 'cpu_usage', 'gauge', '', 'CPU usage'), "
            "('request_duration_seconds', {'instance':'a'}, [(toDateTime64(1000, 3), 0.1)], "
            " 'request_duration_seconds', 'histogram', 'seconds', 'Path \"C:\\\\foo\"\\nnext line\\tλ')"
        )
        node.query(
            "INSERT INTO prometheus "
            "(metric_name, tags, time_series, metric_family, type, unit, help) VALUES "
            "('http_requests_total', {'instance':'c'}, [(toDateTime64(1000, 3), 3)], "
            " 'http_requests_total', 'counter', '', 'Amount of HTTP requests')"
        )
        node.query(
            "CREATE TABLE external_metrics "
            "(metric_family_name String, type String, unit String, help String, "
            "sort_order UInt8, metric_family String) "
            "ENGINE=MergeTree ORDER BY (metric_family_name, sort_order)"
        )
        node.query("CREATE TABLE external_prometheus ENGINE=TimeSeries METRICS external_metrics")
        node.query(
            "INSERT INTO external_metrics VALUES "
            "('a_metric', 'z_type', '', 'z_help', 1, 'a_group_1'), "
            "('a_metric', 'm_type', '', 'm_help', 2, 'a_group_2'), "
            "('a_metric', 'm_type', '', 'm_help', 2, 'a_group_2'), "
            "('a_metric', 'a_type', '', 'a_help', 3, 'a_group_3'), "
            "('b_metric', 'gauge', '', 'B metric', 1, 'b_group')"
        )

        node.query(
            "CREATE TABLE external_selector_samples "
            "(id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) "
            "ENGINE=MergeTree ORDER BY (id, timestamp)"
        )
        node.query(
            "CREATE TABLE external_selector_tags "
            "(id Tuple(UInt64, UUID), metric_name LowCardinality(String), "
            "tags Map(LowCardinality(String), String), "
            "min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
            "max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))) "
            "ENGINE=AggregatingMergeTree PRIMARY KEY metric_name "
            "ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key=1"
        )
        node.query(
            "CREATE TABLE external_selector_prometheus ENGINE=TimeSeries "
            "SAMPLES external_selector_samples TAGS external_selector_tags"
        )
        node.query(
            "CREATE TABLE external_selector_tags_alias "
            "ENGINE=Alias('default', 'external_selector_tags')"
        )
        node.query(
            "CREATE TABLE external_selector_alias_prometheus ENGINE=TimeSeries "
            "SAMPLES external_selector_samples TAGS external_selector_tags_alias"
        )
        node.query(
            "CREATE VIEW external_selector_tags_view AS "
            "SELECT * FROM external_selector_tags"
        )
        node.query(
            "CREATE TABLE external_selector_tags_view_alias "
            "ENGINE=Alias('default', 'external_selector_tags_view')"
        )
        node.query(
            "CREATE TABLE external_selector_view_prometheus ENGINE=TimeSeries "
            "SAMPLES external_selector_samples TAGS external_selector_tags_view"
        )
        node.query(
            "CREATE TABLE external_selector_alias_view_prometheus ENGINE=TimeSeries "
            "SAMPLES external_selector_samples TAGS external_selector_tags_view_alias"
        )
        node.query(
            "INSERT INTO external_selector_tags VALUES "
            "((1, toUUID('00000000-0000-0000-0000-000000000001')), "
            "'protected_metric', {'__name__': 'protected_metric'}, "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        )
        node.query(
            "INSERT INTO external_selector_samples VALUES "
            "((1, toUUID('00000000-0000-0000-0000-000000000001')), "
            "toDateTime64(1000, 3), 42)"
        )
        node.query(
            "CREATE TABLE external_selector_samples_distributed AS external_selector_samples "
            "ENGINE=Distributed('test_cluster', 'default', 'external_selector_samples', rand())"
        )
        node.query(
            "CREATE TABLE external_selector_tags_distributed AS external_selector_tags "
            "ENGINE=Distributed('test_cluster', 'default', 'external_selector_tags', rand())"
        )
        node.query(
            "CREATE TABLE external_selector_distributed_prometheus ENGINE=TimeSeries "
            "SAMPLES external_selector_samples_distributed TAGS external_selector_tags_distributed"
        )

        node.query(
            "CREATE TABLE nullable_metrics "
            "(metric_family_name LowCardinality(String), type Nullable(String), "
            "unit Nullable(String), help Nullable(String), sort_order UInt8) "
            "ENGINE=MergeTree ORDER BY (metric_family_name, sort_order)"
        )
        node.query("CREATE TABLE nullable_prometheus ENGINE=TimeSeries METRICS nullable_metrics")
        node.query(
            "INSERT INTO nullable_metrics VALUES "
            "('nullable_metric', NULL, NULL, 'nullable help', 1), "
            "('escaped\"\\\\metric', 'gauge', 'seconds', 'help \"quoted\"', 1)"
        )

        node.query(
            "CREATE TABLE high_card_metrics "
            "(metric_family_name String, type String, unit String, help String, sort_order UInt32) "
            "ENGINE=MergeTree ORDER BY (metric_family_name, sort_order)"
        )
        node.query("CREATE TABLE high_card_prometheus ENGINE=TimeSeries METRICS high_card_metrics")
        node.query(
            "INSERT INTO high_card_metrics "
            "SELECT 'high_card_metric', concat('type_', toString(number)), "
            "concat('unit_', toString(number)), concat('help_', toString(number)), number "
            "FROM numbers(4000)"
        )

        node.query(
            "CREATE TABLE pruning_metrics "
            "(metric_family_name String, type String, unit String, help String) "
            "ENGINE=MergeTree ORDER BY metric_family_name SETTINGS index_granularity=1"
        )
        node.query("CREATE TABLE pruning_prometheus ENGINE=TimeSeries METRICS pruning_metrics")
        node.query(
            "INSERT INTO pruning_metrics "
            "SELECT concat('metric_', leftPad(toString(number), 5, '0')), "
            "'gauge', '', concat('help_', toString(number)) FROM numbers(5000)"
        )

        node.query(
            "CREATE TABLE wrapped_metrics_source "
            "(metric_family_name String, type String, unit String, help String, version UInt64) "
            "ENGINE=ReplacingMergeTree(version) ORDER BY metric_family_name"
        )
        node.query(
            "CREATE TABLE wrapped_metrics_distributed "
            "AS wrapped_metrics_source "
            "ENGINE=Distributed('test_cluster', 'default', 'wrapped_metrics_source', rand())"
        )
        node.query(
            "CREATE TABLE wrapped_prometheus ENGINE=TimeSeries METRICS wrapped_metrics_distributed"
        )
        node.query(
            "INSERT INTO wrapped_metrics_source VALUES "
            "('wrapped_metric', 'gauge', '', 'old help', 1), "
            "('wrapped_metric', 'gauge', '', 'new help', 2)"
        )

        node.query(
            "CREATE TABLE distributed_plain_metrics_source "
            "(metric_family_name String, type String, unit String, help String) ENGINE=Memory"
        )
        node.query(
            "CREATE TABLE distributed_plain_metrics AS distributed_plain_metrics_source "
            "ENGINE=Distributed('test_cluster', 'default', 'distributed_plain_metrics_source', rand())"
        )
        node.query(
            "CREATE TABLE distributed_plain_prometheus ENGINE=TimeSeries METRICS distributed_plain_metrics"
        )
        node.query(
            "INSERT INTO distributed_plain_metrics_source VALUES "
            "('distributed_plain_metric', 'gauge', '', 'plain help')"
        )

        node.query(
            "CREATE TABLE quota_metrics_source "
            "(metric_family_name String, type String, unit String, help String, version UInt64) "
            "ENGINE=ReplacingMergeTree(version) ORDER BY metric_family_name"
        )
        node.query(
            "CREATE TABLE quota_metrics_distributed AS quota_metrics_source "
            "ENGINE=Distributed('test_cluster', 'default', 'quota_metrics_source', rand())"
        )
        node.query(
            "CREATE TABLE quota_prometheus ENGINE=TimeSeries METRICS quota_metrics_distributed"
        )
        node.query(
            "INSERT INTO quota_metrics_source VALUES "
            "('quota_metric', 'gauge', '', 'quota help', 1)"
        )

        assert node.query("SELECT count() FROM timeSeriesMetrics(prometheus)") == "4\n"
        yield cluster
    finally:
        try:
            node.query("SYSTEM START MERGES")
        finally:
            cluster.shutdown()


def test_metadata_returns_unique_entries():
    data = get_metadata()

    assert list(data) == [
        "cpu_usage",
        "http_requests_total",
        "request_duration_seconds",
    ]
    assert data["cpu_usage"] == [
        {"type": "gauge", "help": "CPU usage", "unit": ""}
    ]
    assert data["http_requests_total"] == [
        {"type": "counter", "help": "Amount of HTTP requests", "unit": ""},
    ]
    assert data["request_duration_seconds"] == [
        {
            "type": "histogram",
            "help": 'Path "C:\\foo"\nnext line\tλ',
            "unit": "seconds",
        }
    ]


def test_metadata_successfully_streams_multiple_result_blocks():
    assert get_metadata({"max_block_size": "1"}) == {
        "cpu_usage": [
            {"type": "gauge", "help": "CPU usage", "unit": ""}
        ],
        "http_requests_total": [
            {"type": "counter", "help": "Amount of HTTP requests", "unit": ""},
        ],
        "request_duration_seconds": [
            {
                "type": "histogram",
                "help": 'Path "C:\\foo"\nnext line\tλ',
                "unit": "seconds",
            }
        ],
    }


def test_metadata_metric_filter():
    assert get_metadata({"metric": "http_requests_total"}) == {
        "http_requests_total": [
            {"type": "counter", "help": "Amount of HTTP requests", "unit": ""},
        ]
    }
    assert get_metadata({"metric": "missing_metric"}) == {}


def test_metadata_metric_filter_uses_primary_key_condition():
    query_id = f"prometheus-metadata-primary-key-test-{uuid.uuid4()}"
    assert get_pruning_metadata(
        {"metric": "metric_04999"},
        headers={"X-ClickHouse-Query-Id": query_id},
    ) == {
        "metric_04999": [
            {"type": "gauge", "help": "help_4999", "unit": ""},
        ]
    }

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    query = node.query(
        "SELECT query FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        "ORDER BY event_time DESC LIMIT 1"
    )
    query = query.replace("\\'", "'")
    read_rows = node.query(
        "SELECT read_rows FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        "ORDER BY event_time DESC LIMIT 1"
    )
    assert (
        "equals(metric_family_name, 'metric_04999')" in query
        or "metric_family_name = 'metric_04999'" in query
    ), query
    assert "equals(ifNull(toString(metric_family_name)," not in query
    assert int(read_rows) < 5000


def test_metadata_works_through_api_v1_handler():
    assert get_metadata(
        {"metric": "cpu_usage"}, path="/combined/api/v1/metadata"
    ) == {"cpu_usage": [{"type": "gauge", "help": "CPU usage", "unit": ""}]}


def test_metadata_works_through_api_v1_handler_with_arbitrary_url_prefix():
    assert get_metadata(
        {"metric": "cpu_usage"}, path="/custom_api/metadata"
    ) == {"cpu_usage": [{"type": "gauge", "help": "CPU usage", "unit": ""}]}


def test_root_api_v1_handler_dispatches_metadata_query_write_and_read():
    assert get_metadata(
        {"metric": "cpu_usage"}, path="/metadata"
    ) == {"cpu_usage": [{"type": "gauge", "help": "CPU usage", "unit": ""}]}

    metric = f"routing_mode_metric_{uuid.uuid4().hex}"
    timestamp = 3000.0
    write_request = convert_time_series_to_protobuf(
        [({"__name__": metric}, {timestamp: 1.0})]
    )
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        write_request,
    )

    response = requests.get(
        f"http://{node.ip_address}:9093/query",
        params={"query": metric, "time": str(timestamp)},
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "success", response.text

    read_request = convert_read_request_to_protobuf(
        f"^{metric}$", timestamp - 1, timestamp + 1
    )
    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "/read",
        read_request,
    )
    assert any(
        label.name == "__name__" and label.value == metric
        for result in read_response.results
        for time_series in result.timeseries
        for label in time_series.labels
    )


def test_metadata_rejects_outer_time_series_row_policy():
    policy_name = f"prometheus_metadata_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.prometheus "
        "FOR SELECT USING metric_family = 'cpu_usage' TO metadata_reader"
    )
    try:
        response = requests.get(
            f"http://{node.ip_address}:9093/api/v1/metadata",
            auth=("metadata_reader", ""),
        )
        assert response.status_code == 400, response.text
        result = response.json()
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result
        assert "row policies" in result["error"], result
        assert "cpu_usage" not in response.text, response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.prometheus")


def test_prometheus_reads_reject_outer_time_series_row_policy():
    policy_name = f"prometheus_read_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.prometheus "
        "FOR SELECT USING metric_family = 'cpu_usage' TO metadata_reader"
    )
    queries = [
        "SELECT count() FROM timeSeriesSamples('default', 'prometheus')",
        "SELECT count() FROM timeSeriesData('default', 'prometheus')",
        "SELECT count() FROM timeSeriesTags('default', 'prometheus')",
        "SELECT count() FROM timeSeriesMetrics('default', 'prometheus')",
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'prometheus', 'http_requests_total', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'prometheus', 'http_requests_total', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'prometheus', 'http_requests_total', 1000, 1000, 1)",
    ]
    try:
        for query in queries:
            response = execute_sql(query, auth=("metadata_reader", ""))
            assert response.status_code != 200, response.text
            assert "row policies" in response.text, response.text

        for path, params in [
            (
                "/api/v1/query",
                {"query": "http_requests_total", "time": "1000"},
            ),
            (
                "/combined/api/v1/query",
                {"query": "http_requests_total", "time": "1000"},
            ),
            (
                "/combined/api/v1/query_range",
                {"query": "http_requests_total", "start": "1000", "end": "1001", "step": "1"},
            ),
        ]:
            response = requests.get(
                f"http://{node.ip_address}:9093{path}",
                params=params,
                auth=("metadata_reader", ""),
            )
            assert response.status_code == 400, response.text
            result = response.json()
            assert result["status"] == "error", result
            assert result["errorType"] == "bad_data", result
            assert "row policies" in result["error"], result
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.prometheus")


def test_selector_rejects_external_target_row_policy():
    queries = [
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'external_selector_prometheus', 'protected_metric', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'external_selector_prometheus', 'protected_metric', 1000, 1000, 1)",
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_alias_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'external_selector_alias_prometheus', 'protected_metric', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'external_selector_alias_prometheus', 'protected_metric', 1000, 1000, 1)",
    ]
    auth = ("metadata_external_selector_reader", "")

    for query in queries:
        response = execute_sql(query, auth=auth)
        assert response.status_code == 200, response.text

    policy_name = f"external_selector_tags_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.external_selector_tags "
        "FOR SELECT USING 0 TO metadata_external_selector_reader"
    )
    try:
        for query in queries:
            response = execute_sql(query, auth=auth)
            assert response.status_code != 200, response.text
            assert "row policies" in response.text, response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.external_selector_tags")


def test_selector_rejects_view_target_row_policy():
    queries = [
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_view_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'external_selector_view_prometheus', 'protected_metric', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'external_selector_view_prometheus', 'protected_metric', 1000, 1000, 1)",
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_alias_view_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'external_selector_alias_view_prometheus', 'protected_metric', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'external_selector_alias_view_prometheus', 'protected_metric', 1000, 1000, 1)",
    ]
    auth = ("metadata_external_selector_reader", "")
    policy_name = f"external_selector_view_tags_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.external_selector_tags "
        "FOR SELECT USING 0 TO metadata_external_selector_reader"
    )
    try:
        for query in queries:
            response = execute_sql(query, auth=auth)
            assert response.status_code != 200, response.text
            assert "view targets" in response.text.lower(), response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.external_selector_tags")


def test_metadata_final_probe_does_not_consume_query_quota():
    user_name = f"metadata_probe_quota_reader_{uuid.uuid4().hex}"
    quota_name = f"metadata_probe_quota_{uuid.uuid4().hex}"
    auth = (user_name, "")

    try:
        node.query(
            f"CREATE USER {user_name} SETTINGS allow_experimental_time_series_table = 1"
        )
        node.query(f"GRANT SELECT ON default.quota_prometheus TO {user_name}")
        node.query(
            f"CREATE QUOTA {quota_name} FOR INTERVAL 1 HOUR MAX QUERIES 1 TO {user_name}"
        )
        assert get_quota_metadata(
            {"metric": "quota_metric"}, auth=auth
        ) == {
            "quota_metric": [
                {"type": "gauge", "help": "quota help", "unit": ""},
            ]
        }
    finally:
        node.query(f"DROP QUOTA IF EXISTS {quota_name}")
        node.query(f"DROP USER IF EXISTS {user_name}")


def test_selector_preserves_active_roles_for_distributed_targets():
    user_name = f"metadata_external_selector_sql_reader_{uuid.uuid4().hex}"
    role_name = f"metadata_external_selector_role_{uuid.uuid4().hex}"
    policy_name = f"external_selector_distributed_tags_policy_{uuid.uuid4().hex}"
    auth = (user_name, "")
    queries = [
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_distributed_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'external_selector_distributed_prometheus', 'protected_metric', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'external_selector_distributed_prometheus', 'protected_metric', 1000, 1000, 1)",
    ]

    node.query(
        f"CREATE USER {user_name} SETTINGS allow_experimental_time_series_table = 1"
    )
    try:
        for table_name in [
            "external_selector_prometheus",
            "external_selector_distributed_prometheus",
        ]:
            node.query(f"GRANT SELECT ON default.{table_name} TO {user_name}")
        node.query(f"CREATE ROLE {role_name}")
        node.query(f"GRANT {role_name} TO {user_name}")
        node.query(
            f"CREATE ROW POLICY {policy_name} ON default.external_selector_tags "
            f"FOR SELECT USING 0 TO {role_name}"
        )

        for query in queries:
            response = execute_sql(query, auth=auth)
            assert response.status_code == 200, response.text
            assert response.text.strip() == "1", response.text

            response = execute_sql(query, auth=auth, params={"role": role_name})
            assert response.status_code == 200, response.text
            assert response.text.strip() == "0", response.text
    finally:
        node.query(f"DROP ROW POLICY IF EXISTS {policy_name} ON default.external_selector_tags")
        node.query(f"REVOKE {role_name} FROM {user_name}")
        node.query(f"DROP ROLE IF EXISTS {role_name}")
        node.query(f"DROP USER IF EXISTS {user_name}")


def test_distributed_targets_use_logical_table_grants():
    user_name = f"metadata_external_selector_outer_only_{uuid.uuid4().hex}"
    auth = (user_name, "")
    queries = [
        ("SELECT count() > 0 FROM timeSeriesSamples('default', 'external_selector_distributed_prometheus')", "1"),
        ("SELECT count() > 0 FROM timeSeriesTags('default', 'external_selector_distributed_prometheus')", "1"),
        ("SELECT count() > 0 FROM timeSeriesMetrics('default', 'wrapped_prometheus')", "1"),
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'external_selector_distributed_prometheus', 'protected_metric', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))",
            "1",
        ),
        (
            "SELECT count() FROM prometheusQuery('default', 'external_selector_distributed_prometheus', 'protected_metric', 1000)",
            "1",
        ),
        (
            "SELECT count() FROM prometheusQueryRange("
            "'default', 'external_selector_distributed_prometheus', 'protected_metric', 1000, 1000, 1)",
            "1",
        ),
    ]

    node.query(
        f"CREATE USER {user_name} SETTINGS allow_experimental_time_series_table = 1"
    )
    try:
        node.query(
            f"GRANT SELECT ON default.external_selector_distributed_prometheus TO {user_name}"
        )
        node.query(f"GRANT SELECT ON default.wrapped_prometheus TO {user_name}")

        for query, expected in queries:
            response = execute_sql(query, auth=auth)
            assert response.status_code == 200, response.text
            assert response.text.strip() == expected, response.text

        assert get_wrapped_metadata({"metric": "wrapped_metric"}, auth=auth) == {
            "wrapped_metric": [
                {"type": "gauge", "help": "new help", "unit": ""},
            ]
        }
    finally:
        node.query(f"DROP USER IF EXISTS {user_name}")


def test_metadata_is_stable_after_metrics_merge():
    before_merge = get_metadata({"metric": "http_requests_total"})
    optimize_table_final("prometheus")
    assert get_metadata({"metric": "http_requests_total"}) == before_merge


def test_metadata_limit():
    assert list(get_metadata({"limit": "1"})) == ["cpu_usage"]
    assert get_metadata({"limit": "0"}) == {}
    assert len(get_metadata({"limit": "-1"})) == 3
    assert len(get_metadata({"limit": ""})) == 3


def test_metadata_limit_per_metric():
    data = get_metadata({"limit_per_metric": "1"})
    assert data["http_requests_total"] == [
        {"type": "counter", "help": "Amount of HTTP requests", "unit": ""}
    ]

    # Prometheus treats 0 and negative values as unlimited for limit_per_metric.
    assert len(get_metadata({"limit_per_metric": "0"})["http_requests_total"]) == 1
    assert len(get_metadata({"limit_per_metric": "-1"})["http_requests_total"]) == 1
    assert len(get_metadata({"limit_per_metric": ""})["http_requests_total"]) == 1


def test_metadata_combined_limits():
    data = get_metadata({"limit": "2", "limit_per_metric": "1"})
    assert list(data) == ["cpu_usage", "http_requests_total"]
    assert all(len(metadata) == 1 for metadata in data.values())


def test_metadata_limited_query_handles_empty_result_and_zero_limit():
    assert get_metadata({"metric": "missing_metric", "limit_per_metric": "1"}) == {}
    assert get_metadata(
        {"limit": "0", "limit_per_metric": "1", "max_block_size": "1"}
    ) == {}


def test_metadata_reports_exception_during_result_pull():
    response = requests.get(
        f"http://{node.ip_address}:9093/pruning/api/v1/metadata",
        params={
            "max_threads": "1",
            "max_block_size": "1",
            "max_result_rows": "1",
            "result_overflow_mode": "throw",
        },
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert "Limit for result exceeded" in result["error"], result


def test_metadata_aborts_stream_when_result_pull_fails_after_response_is_sent():
    with requests.get(
        f"http://{node.ip_address}:9093/pruning/api/v1/metadata",
        params={
            "max_threads": "1",
            "max_block_size": "1",
            "max_result_rows": "1",
            "result_overflow_mode": "throw",
            "http_response_buffer_size": "1",
        },
        stream=True,
    ) as response:
        assert response.status_code == 200, response.text
        assert response.headers.get("Transfer-Encoding") == "chunked", response.headers

        received = b""
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            for piece in response.iter_content(chunk_size=None):
                received += piece

        assert received.startswith(b'{"status":"success","data":{'), received
        assert b"metric_" in received, received
        assert b"__exception__" in received, received
        assert b"Limit for result exceeded" in received, received


def test_external_metadata_sorts_all_unique_entries():
    assert get_external_metadata({"metric": "a_metric"}) == {
        "a_metric": [
            {"type": "a_type", "help": "a_help", "unit": ""},
            {"type": "m_type", "help": "m_help", "unit": ""},
            {"type": "z_type", "help": "z_help", "unit": ""},
        ]
    }


def test_external_metadata_normalizes_nullable_columns_and_escapes_metric_family():
    assert get_nullable_metadata() == {
        "escaped\"\\metric": [
            {"type": "gauge", "help": 'help "quoted"', "unit": "seconds"},
        ],
        "nullable_metric": [
            {"type": "", "help": "nullable help", "unit": ""},
        ],
    }
    assert get_nullable_metadata({"metric": 'escaped"\\metric'}) == {
        "escaped\"\\metric": [
            {"type": "gauge", "help": 'help "quoted"', "unit": "seconds"},
        ]
    }


def test_external_metadata_limit_per_metric_returns_bounded_entries():
    expected = get_external_metadata({"metric": "a_metric"})["a_metric"]
    for limit in [1, 2]:
        assert_metadata_subset(
            get_external_metadata(
                {"metric": "a_metric", "limit_per_metric": str(limit)}
            ),
            "a_metric",
            expected,
            limit,
        )


@pytest.mark.parametrize("limit_per_metric", [None, "0", "2"])
def test_external_metadata_deduplicates_exact_duplicate_rows(limit_per_metric):
    params = {"metric": "a_metric"}
    if limit_per_metric is not None:
        params["limit_per_metric"] = limit_per_metric

    expected = [
        {"type": "a_type", "help": "a_help", "unit": ""},
        {"type": "m_type", "help": "m_help", "unit": ""},
        {"type": "z_type", "help": "z_help", "unit": ""},
    ]
    result = get_external_metadata(params)
    assert_metadata_subset(
        result,
        "a_metric",
        expected,
        2 if limit_per_metric == "2" else len(expected),
    )
    if limit_per_metric != "2":
        assert len(result["a_metric"]) == len(expected)


@pytest.mark.parametrize("limit_per_metric", ["0", "-1", "10"])
def test_external_metadata_non_restrictive_limit_per_metric_returns_all_entries(limit_per_metric):
    expected = get_external_metadata({"metric": "a_metric"})
    assert get_external_metadata(
        {"metric": "a_metric", "limit_per_metric": limit_per_metric}
    ) == expected


def test_external_metadata_combined_limits():
    expected = get_external_metadata({"metric": "a_metric"})["a_metric"]
    assert_metadata_subset(
        get_external_metadata({"limit": "1", "limit_per_metric": "1"}),
        "a_metric",
        expected,
        1,
    )


def test_external_metadata_limit_per_metric_remains_bounded_after_optimize():
    params = {"metric": "a_metric", "limit_per_metric": "2"}
    expected = get_external_metadata({"metric": "a_metric"})["a_metric"]
    assert_metadata_subset(get_external_metadata(params), "a_metric", expected, 2)
    optimize_table_final("external_metrics")
    assert_metadata_subset(get_external_metadata(params), "a_metric", expected, 2)


def test_metadata_aliases_are_safe_with_prefer_column_name_to_alias():
    assert get_external_metadata(
        {"metric": "a_metric", "prefer_column_name_to_alias": "1"}
    ) == {
        "a_metric": [
            {"type": "a_type", "help": "a_help", "unit": ""},
            {"type": "m_type", "help": "m_help", "unit": ""},
            {"type": "z_type", "help": "z_help", "unit": ""},
        ]
    }
    assert_metadata_subset(
        get_external_metadata(
            {
                "metric": "a_metric",
                "limit_per_metric": "2",
                "prefer_column_name_to_alias": "1",
            }
        ),
        "a_metric",
        [
            {"type": "a_type", "help": "a_help", "unit": ""},
            {"type": "m_type", "help": "m_help", "unit": ""},
            {"type": "z_type", "help": "z_help", "unit": ""},
        ],
        2,
    )


def test_high_cardinality_metadata_limit_returns_bounded_entries():
    params = {"metric": "high_card_metric", "limit_per_metric": "2"}

    def assert_bounded_result(data):
        entries = data["high_card_metric"]
        assert 0 < len(entries) <= 2
        assert (
            len({(entry["type"], entry["help"], entry["unit"]) for entry in entries})
            == len(entries)
        )
        assert all(
            entry["type"].startswith("type_")
            and entry["help"].startswith("help_")
            and entry["unit"].startswith("unit_")
            for entry in entries
        )

    assert_bounded_result(get_high_card_metadata(params))
    optimize_table_final("high_card_metrics")
    assert_bounded_result(get_high_card_metadata(params))


def test_limited_metadata_query_uses_bounded_unique_aggregation():
    query_id = f"prometheus-metadata-limited-query-shape-test-{uuid.uuid4()}"
    get_high_card_metadata(
        {
            "metric": "high_card_metric",
            "limit_per_metric": "2",
            "max_rows_to_group_by": "2",
        },
        headers={"X-ClickHouse-Query-Id": query_id},
    )

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    query = node.query(
        "SELECT query FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        "ORDER BY event_time DESC LIMIT 1"
    )
    assert "groupUniqArray(2)" in query
    assert "LIMIT 2 BY" not in query


def test_wrapped_replacing_metrics_target_uses_final():
    assert get_wrapped_metadata({"metric": "wrapped_metric"}) == {
        "wrapped_metric": [
            {"type": "gauge", "help": "new help", "unit": ""},
        ]
    }


def test_distributed_final_success_is_logged_as_user_query():
    query_id = f"prometheus-distributed-final-success-test-{uuid.uuid4()}"
    assert get_wrapped_metadata(
        {"metric": "wrapped_metric"},
        headers={"X-ClickHouse-Query-Id": query_id},
    ) == {
        "wrapped_metric": [
            {"type": "gauge", "help": "new help", "unit": ""},
        ]
    }

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )


def test_distributed_non_final_metrics_target_falls_back_without_final():
    assert get_distributed_plain_metadata({"metric": "distributed_plain_metric"}) == {
        "distributed_plain_metric": [
            {"type": "gauge", "help": "plain help", "unit": ""},
        ]
    }


def test_distributed_final_fallback_does_not_log_the_swallowed_exception():
    query_id = f"prometheus-distributed-final-fallback-test-{uuid.uuid4()}"
    assert get_distributed_plain_metadata(
        {"metric": "distributed_plain_metric"},
        headers={"X-ClickHouse-Query-Id": query_id},
    ) == {
        "distributed_plain_metric": [
            {"type": "gauge", "help": "plain help", "unit": ""},
        ]
    }

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    assert node.query(
        "SELECT count() FROM system.query_log "
        f"WHERE type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') "
        f"AND query_id = '{query_id}'"
    ) == "0\n"


def test_distributed_zero_limit_does_not_use_final():
    query_id = f"prometheus-distributed-zero-limit-test-{uuid.uuid4()}"
    assert get_distributed_plain_metadata(
        {"limit": "0"},
        headers={"X-ClickHouse-Query-Id": query_id},
    ) == {}

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    query = node.query(
        "SELECT query FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        "ORDER BY event_time DESC LIMIT 1"
    )
    assert "FINAL" not in query.upper(), query


def test_external_metadata_allows_select_on_outer_time_series_table_only():
    assert get_external_metadata(
        {"metric": "a_metric"}, auth=("metadata_external_reader", "")
    ) == {
        "a_metric": [
            {"type": "a_type", "help": "a_help", "unit": ""},
            {"type": "m_type", "help": "m_help", "unit": ""},
            {"type": "z_type", "help": "z_help", "unit": ""},
        ]
    }


def test_external_metadata_rejects_target_row_policy():
    policy_name = f"external_metrics_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.external_metrics "
        "FOR SELECT USING metric_family_name = 'a_metric' TO metadata_external_reader"
    )
    try:
        response = execute_sql(
            "SELECT count() FROM timeSeriesMetrics('default', 'external_prometheus')",
            auth=("metadata_external_reader", ""),
        )
        assert response.status_code != 200, response.text
        assert "row policies" in response.text, response.text
        assert "external_metrics" not in response.text, response.text

        response = requests.get(
            f"http://{node.ip_address}:9093/external/api/v1/metadata",
            auth=("metadata_external_reader", ""),
        )
        assert response.status_code == 400, response.text
        result = response.json()
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result
        assert "row policies" in result["error"], result
        assert "external_metrics" not in response.text, response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.external_metrics")


@pytest.mark.parametrize("name", ["limit", "limit_per_metric"])
def test_metadata_invalid_limit(name):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/metadata",
        params={name: "not-a-number"},
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert f"{name} must be a number" in result["error"], result
    assert "Unknown setting" not in result["error"], result


@pytest.mark.parametrize(
    "name, value",
    [
        ("limit", "9223372036854775807"),
        ("limit", "-9223372036854775808"),
        ("limit_per_metric", "9223372036854775807"),
        ("limit_per_metric", "-9223372036854775808"),
    ],
)
def test_metadata_accepts_int64_limit_boundaries(name, value):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/metadata",
        params={name: value},
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "success", response.text


@pytest.mark.parametrize(
    "name, value",
    [
        ("limit", "9223372036854775808"),
        ("limit", "-9223372036854775809"),
        ("limit", "1.0"),
        ("limit_per_metric", "9223372036854775808"),
        ("limit_per_metric", "-9223372036854775809"),
        ("limit_per_metric", "1.0"),
    ],
)
def test_metadata_rejects_out_of_range_or_non_integer_limits(name, value):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/metadata",
        params={name: value},
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert f"{name} must be a number" in result["error"], result


@pytest.mark.parametrize("method", ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"])
def test_metadata_method_contract(method):
    request_kwargs = {"params": {"metric": "cpu_usage"}}
    if method == "POST":
        request_kwargs = {"data": {"metric": "cpu_usage"}}
    elif method == "OPTIONS":
        request_kwargs["headers"] = {
            "Origin": "https://example.test",
            "Access-Control-Request-Method": "GET",
        }

    response = requests.request(
        method,
        f"http://{node.ip_address}:9093/api/v1/metadata",
        **request_kwargs,
    )

    if method == "GET":
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "success", response.text
    elif method == "HEAD":
        assert response.status_code == 200, response.text
        assert response.content == b""
    elif method == "OPTIONS":
        assert response.status_code == 204, response.text
        assert response.content == b""
        assert response.headers.get("Access-Control-Allow-Origin") == "https://example.test", response.headers
    else:
        assert response.status_code == 405, response.text
        assert response.headers.get("Allow") == "GET, HEAD", response.headers
        result = response.json()
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE"])
def test_metadata_method_rejection_precedes_setting_validation(method):
    response = requests.request(
        method,
        f"http://{node.ip_address}:9093/api/v1/metadata",
        params={"definitely_not_a_setting": "1"},
    )
    assert response.status_code == 405, response.text
    assert response.headers.get("Allow") == "GET, HEAD", response.headers


def test_query_head_method_contract():
    response = requests.head(
        f"http://{node.ip_address}:9093/combined/api/v1/query",
        params={"query": "http_requests_total", "time": "1000"},
    )
    assert response.status_code == 200, response.text
    assert response.content == b""


@pytest.mark.parametrize(
    "path",
    [
        "/combined/api/v1/metadata",
        "/combined/api/v1/write",
        "/combined/api/v1/read",
        "/custom_api/metadata",
    ],
)
def test_prometheus_options_preflight_uses_clickhouse_headers_without_authentication(path):
    response = requests.options(
        f"http://{node.ip_address}:9093{path}",
        headers={
            "Origin": "https://example.test",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Authorization",
        },
    )
    assert response.status_code == 204, response.text
    assert response.content == b""
    assert response.headers.get("Access-Control-Allow-Origin") == "https://example.test", response.headers


def test_dedicated_query_handler_supports_custom_endpoint():
    response = requests.get(
        f"http://{node.ip_address}:9093/custom/query",
        params={"query": "http_requests_total", "time": "1000"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "success", response.text


def test_metadata_limit_is_endpoint_local_but_query_limit_remains_a_setting():
    assert list(get_metadata({"limit": "1"})) == ["cpu_usage"]

    response = requests.get(
        f"http://{node.ip_address}:9093/combined/api/v1/query",
        params={"query": "http_requests_total", "time": "1000", "limit": "1"},
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "success", result
    assert len(result["data"]["result"]) == 1, result


def test_metadata_api_appears_in_query_log():
    query_id = f"prometheus-metadata-query-log-test-{uuid.uuid4()}"
    get_metadata(headers={"X-ClickHouse-Query-Id": query_id})

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        f"AND read_rows > 0 AND read_bytes > 0",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )


def test_metadata_api_accepts_query_id_parameter():
    query_id = f"prometheus-metadata-query-parameter-test-{uuid.uuid4()}"
    get_metadata(params={"query_id": query_id})

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        f"AND read_rows > 0 AND read_bytes > 0",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )


def test_query_and_metadata_allow_access_through_time_series_table():
    auth = ("metadata_reader", "")

    assert get_metadata({"metric": "http_requests_total"}, auth=auth) == {
        "http_requests_total": [
            {"type": "counter", "help": "Amount of HTTP requests", "unit": ""},
        ]
    }

    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/query",
        params={"query": "http_requests_total", "time": "1000"},
        auth=auth,
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "success", response.text


def test_combined_handler_dispatches_remote_write_and_read():
    metric = f"combined_router_metric_{uuid.uuid4().hex}"
    timestamp = 2000.0
    write_request = convert_time_series_to_protobuf(
        [({"__name__": metric}, {timestamp: 1.0})]
    )
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/combined/api/v1/write",
        write_request,
    )

    read_request = convert_read_request_to_protobuf(
        f"^{metric}$", timestamp - 1, timestamp + 1
    )
    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "/combined/api/v1/read",
        read_request,
    )
    assert any(
        label.name == "__name__" and label.value == metric
        for result in read_response.results
        for time_series in result.timeseries
        for label in time_series.labels
    )


def test_combined_handler_writes_remote_metadata_to_metadata_api():
    metric = f"remote_write_metadata_metric_{uuid.uuid4().hex}"
    write_request = remote_pb2.WriteRequest()
    write_request.metadata.add(
        metric_family_name=metric,
        type=types_pb2.MetricMetadata.GAUGEHISTOGRAM,
        help="Gauge histogram metadata",
        unit="requests",
    )

    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/combined/api/v1/write",
        write_request,
    )

    assert get_metadata(
        {"metric": metric}, path="/combined/api/v1/metadata"
    ) == {
        metric: [
            {
                "type": "gaugehistogram",
                "help": "Gauge histogram metadata",
                "unit": "requests",
            }
        ]
    }


@pytest.mark.parametrize(
    "query",
    [
        "SELECT count() FROM timeSeriesSamples('default', 'prometheus')",
        "SELECT count() FROM timeSeriesData('default', 'prometheus')",
        "SELECT count() FROM timeSeriesTags('default', 'prometheus')",
        "SELECT count() FROM timeSeriesMetrics('default', 'prometheus')",
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'prometheus', 'http_requests_total', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'prometheus', 'http_requests_total', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'prometheus', 'http_requests_total', 1000, 1000, 1)",
    ],
)
def test_time_series_table_functions_allow_select_only_user(query):
    expected = node.query(query)
    response = execute_sql(query, auth=("metadata_reader", ""))
    assert response.status_code == 200, response.text
    assert response.text == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT count() FROM timeSeriesSamples('default', 'prometheus')",
        "SELECT count() FROM timeSeriesData('default', 'prometheus')",
        "SELECT count() FROM timeSeriesTags('default', 'prometheus')",
        "SELECT count() FROM timeSeriesMetrics('default', 'prometheus')",
        (
            "SELECT count() FROM timeSeriesSelector("
            "'default', 'prometheus', 'http_requests_total', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "SELECT count() FROM prometheusQuery('default', 'prometheus', 'http_requests_total', 1000)",
        "SELECT count() FROM prometheusQueryRange("
        "'default', 'prometheus', 'http_requests_total', 1000, 1000, 1)",
    ],
)
def test_time_series_table_functions_require_select_on_time_series_table(query):
    response = execute_sql(query, auth=("metadata_temp_table_only", ""))
    assert response.status_code != 200, response.text
    assert "SELECT" in response.text, response.text
    assert "default.prometheus" in response.text, response.text


@pytest.mark.parametrize(
    "query",
    [
        "DESCRIBE TABLE timeSeriesSamples('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesData('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesTags('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesMetrics('default', 'prometheus')",
        (
            "DESCRIBE TABLE timeSeriesSelector("
            "'default', 'prometheus', 'http_requests_total', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "DESCRIBE TABLE prometheusQuery('default', 'prometheus', 'http_requests_total', 1000)",
        "DESCRIBE TABLE prometheusQueryRange("
        "'default', 'prometheus', 'http_requests_total', 1000, 1000, 1)",
    ],
)
def test_time_series_table_function_schema_requires_select_on_time_series_table(query):
    response = execute_sql(query, auth=("metadata_temp_table_only", ""))
    assert response.status_code != 200, response.text
    assert "SELECT" in response.text, response.text
    assert "default.prometheus" in response.text, response.text


def test_time_series_table_function_schema_inference_requires_select():
    response = execute_sql(
        "CREATE TEMPORARY TABLE inferred_metadata AS "
        "timeSeriesMetrics('default', 'prometheus')",
        auth=("metadata_insert_temp_table_only", ""),
    )
    assert response.status_code != 200, response.text
    assert "SELECT" in response.text, response.text
    assert "default.prometheus" in response.text, response.text


def test_time_series_table_function_schema_allows_select_only_user():
    response = execute_sql(
        "DESCRIBE TABLE timeSeriesMetrics('default', 'prometheus')",
        auth=("metadata_reader", ""),
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "query",
    [
        "DESCRIBE TABLE timeSeriesSamples('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesData('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesTags('default', 'prometheus')",
        "DESCRIBE TABLE timeSeriesMetrics('default', 'prometheus')",
        (
            "DESCRIBE TABLE timeSeriesSelector("
            "'default', 'prometheus', 'http_requests_total', "
            "toDateTime64(1000, 3), toDateTime64(1000, 3))"
        ),
        "DESCRIBE TABLE prometheusQuery('default', 'prometheus', 'http_requests_total', 1000)",
        "DESCRIBE TABLE prometheusQueryRange("
        "'default', 'prometheus', 'http_requests_total', 1000, 1000, 1)",
    ],
)
def test_time_series_table_function_schema_allows_select_with_outer_row_policy(query):
    policy_name = f"prometheus_schema_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.prometheus "
        "FOR SELECT USING metric_family = 'cpu_usage' TO metadata_select_temp_table_only"
    )
    try:
        response = execute_sql(query, auth=("metadata_select_temp_table_only", ""))
        assert response.status_code == 200, response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.prometheus")


def test_time_series_table_function_schema_inference_allows_select_with_outer_row_policy():
    policy_name = f"prometheus_schema_inference_policy_{uuid.uuid4().hex}"
    temporary_table_name = f"inferred_metadata_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.prometheus "
        "FOR SELECT USING metric_family = 'cpu_usage' TO metadata_select_temp_table_only"
    )
    try:
        response = execute_sql(
            f"CREATE TEMPORARY TABLE {temporary_table_name} AS "
            "SELECT * FROM timeSeriesMetrics('default', 'prometheus')",
            auth=("metadata_select_temp_table_only", ""),
        )
        assert response.status_code == 200, response.text
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.prometheus")


def test_time_series_table_function_insert_requires_insert_access():
    response = execute_sql(
        "INSERT INTO TABLE FUNCTION timeSeriesSamples('default', 'prometheus') "
        "SELECT id, timestamp, value "
        "FROM timeSeriesSamples('default', 'prometheus') LIMIT 0",
        auth=("metadata_select_temp_table_only", ""),
    )
    assert response.status_code != 200, response.text
    assert "INSERT" in response.text, response.text
    assert "default.prometheus" in response.text, response.text


def test_time_series_table_function_insert_allows_insert_only_user():
    response = execute_sql(
        "INSERT INTO TABLE FUNCTION timeSeriesSamples('default', 'prometheus') "
        "SELECT tuple(toUInt64(0), toUUID('00000000-0000-0000-0000-000000000001')), "
        "toDateTime64(2000, 3), 1.0",
        auth=("metadata_insert_temp_table_only", ""),
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "path, params",
    [
        ("/combined/api/v1/query", {"query": "http_requests_total", "time": "1000"}),
        (
            "/combined/api/v1/query_range",
            {"query": "http_requests_total", "start": "1000", "end": "1010", "step": "1"},
        ),
        ("/combined/api/v1/metadata", {}),
        ("/combined/api/v1/metadata?limit=0", {}),
        ("/combined/api/v1/metadata?metric=missing_metric", {}),
        ("/combined/api/v1/series", {"match[]": "{__name__=\"http_requests_total\"}"}),
        ("/combined/api/v1/labels", {}),
        ("/combined/api/v1/label/instance/values", {}),
    ],
)
def test_prometheus_storage_endpoints_require_select_on_time_series_table(path, params):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        params=params,
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert "SELECT" in result["error"], result
    assert "default.prometheus" in result["error"], result


@pytest.mark.parametrize(
    "path, expected_error",
    [
        ("/combined/api/v1/format_query", "format_query endpoint is not implemented"),
        ("/combined/api/v1/parse_query", "parse_query endpoint is not implemented"),
    ],
)
def test_prometheus_syntax_endpoints_do_not_require_select_on_time_series_table(path, expected_error):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert expected_error in result["error"], result
    assert "default.prometheus" not in response.text, response.text


@pytest.mark.parametrize(
    "method, parameters",
    [
        ("GET", {}),
        ("GET", {"metric": "foo"}),
        ("GET", {"definitely_not_a_setting": "1"}),
        ("POST", {"metric": "foo"}),
        ("POST", {"definitely_not_a_setting": "1"}),
        ("OPTIONS", {}),
    ],
)
def test_unknown_endpoint_returns_not_found_before_table_authorization(method, parameters):
    request_kwargs = {"params": parameters} if method in ("GET", "OPTIONS") else {"data": parameters}
    response = requests.request(
        method,
        f"http://{node.ip_address}:9093/combined/api/v1/does-not-exist",
        auth=("metadata_temp_table_only", ""),
        **request_kwargs,
    )
    assert response.status_code == 404, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "not_found", result
    assert "default.prometheus" not in response.text, response.text


@pytest.mark.parametrize(
    "path",
    [
        "/combined/api/v1/extra/metadata",
        "/combined/api/v1/extra/query",
        "/combined/api/v1/extra/query_range",
        "/combined/api/v1/extra/labels",
        "/combined/api/v1/extra/label/instance/values",
        "/combined/api/v1/extra/write",
        "/combined/api/v1/extra/read",
    ],
)
def test_combined_handler_does_not_match_endpoint_suffixes(path):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 404, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "not_found", result
    assert "default.prometheus" not in response.text, response.text


@pytest.mark.parametrize(
    "path, params, expected_status, expected_error",
    [
        (
            "/combined/api/v1/query",
            {"query": "http_requests_total", "time": "1000"},
            200,
            None,
        ),
        (
            "/combined/api/v1/query_range",
            {"query": "http_requests_total", "start": "1000", "end": "1001", "step": "1"},
            200,
            None,
        ),
        ("/combined/api/v1/format_query", {}, 400, "format_query endpoint is not implemented"),
        ("/combined/api/v1/parse_query", {}, 400, "parse_query endpoint is not implemented"),
        (
            "/combined/api/v1/series",
            {"match[]": '{__name__="http_requests_total"}'},
            400,
            "series endpoint is not implemented",
        ),
        (
            "/combined/api/v1/labels",
            {"match[]": '{__name__="http_requests_total"}'},
            400,
            "labels endpoint is not implemented",
        ),
        (
            "/combined/api/v1/label/instance/values",
            {"match[]": '{__name__="http_requests_total"}'},
            400,
            "label values endpoint is not implemented",
        ),
    ],
)
def test_combined_handler_dispatches_known_endpoints(path, params, expected_status, expected_error):
    response = requests.get(f"http://{node.ip_address}:9093{path}", params=params)
    assert response.status_code == expected_status, response.text
    result = response.json()

    if expected_error is None:
        assert result["status"] == "success", result
    else:
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result
        assert expected_error in result["error"], result


@pytest.mark.parametrize(
    "path",
    [
        "/combined/api/v1/labels/values",
        "/values",
        "/combined/api/v1/label//values",
        "/combined/api/v1/label/instance/not-values",
    ],
)
def test_malformed_label_values_endpoints_return_not_found(path):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 404, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "not_found", result
