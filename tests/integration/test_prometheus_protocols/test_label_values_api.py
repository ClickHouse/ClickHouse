"""Tests for the Prometheus /api/v1/label/<name>/values endpoint."""

import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import convert_time_series_to_protobuf, send_protobuf_to_remote_write


cluster = ClickHouseCluster(__file__)
MAIN_HTTP_PORT = 8123

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/prometheus_metadata_users.xml",
    ],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    time_series = [
        (
            {
                "__name__": "cpu_usage",
                "host": "server1",
                "datacenter": "us-east",
                "http.status_code": "200",
                "U__bad__utf_D900_": "surrogate_literal",
                "U__my_00000061_": "overlong_literal",
            },
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {
                "__name__": "cpu_usage",
                "host": "server2",
                "datacenter": "us-west",
                "http.status_code": "500",
            },
            {1000: 0.3, 1015: 0.4, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
    ]
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", convert_time_series_to_protobuf(time_series))


def get_json_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data


def execute_sql(query, auth):
    return requests.post(
        f"http://{node.ip_address}:{MAIN_HTTP_PORT}",
        data=query,
        auth=auth,
    )


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prometheus_no_filter ENGINE=TimeSeries "
            "SETTINGS tags_to_columns = {'region': 'label_value'}"
        )
        node.query(
            "INSERT INTO prometheus_no_filter (metric_name, tags, time_series) "
            "SELECT concat('cardinality_metric_', toString(number)), "
            "map('region', if(number % 2 = 0, 'eu', 'us'), 'zone', if(number % 2 = 0, 'a', 'b')), "
            "[(toDateTime64(1000, 3), toFloat64(number))] "
            "FROM numbers(1000)"
        )
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_label_values_returns_metric_names():
    data = get_json_from_api("/api/v1/label/__name__/values")["data"]
    assert data == ["cpu_usage", "memory_usage"]


def test_label_values_returns_distinct_tag_values():
    data = get_json_from_api("/api/v1/label/host/values")["data"]
    assert data == ["server1", "server2"]


def test_label_values_decodes_prometheus_value_encoding():
    data = get_json_from_api("/api/v1/label/U__http_2e_status__code/values")["data"]
    assert data == ["200", "500"]


def test_label_values_keeps_malformed_escaped_names_literal():
    assert get_json_from_api("/api/v1/label/U__bad__utf_D900_/values")["data"] == ["surrogate_literal"]
    assert get_json_from_api("/api/v1/label/U__my_00000061_/values")["data"] == ["overlong_literal"]


def test_label_values_rejects_empty_decoded_name():
    response = requests.get(f"http://{node.ip_address}:9093/api/v1/label/U__/values")
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error"
    assert result["errorType"] == "bad_data"


def test_label_values_match_filter():
    data = get_json_from_api("/api/v1/label/host/values?match[]=memory_usage")["data"]
    assert data == ["server1"]


def test_label_values_for_unknown_label_is_empty():
    assert get_json_from_api("/api/v1/label/missing/values")["data"] == []


def test_label_values_avoids_source_column_alias_collision():
    data = get_json_from_api(
        "/no_filter/api/v1/label/zone/values",
        params={"prefer_column_name_to_alias": 1},
    )["data"]
    assert data == ["a", "b"]


def test_label_values_time_range_filters_series():
    assert get_json_from_api("/api/v1/label/host/values?start=2000&end=3000")["data"] == []


def test_label_values_ignores_empty_optional_parameters():
    data = get_json_from_api("/api/v1/label/host/values?start=&end=&limit=")["data"]
    assert data == ["server1", "server2"]


def test_label_values_limit_reports_truncation():
    data = get_json_from_api("/api/v1/label/host/values?limit=1")
    assert data["data"] == ["server1"]
    assert data["warnings"] == ["results truncated due to limit"]


def test_label_values_accepts_signed_int64_max_limit():
    data = get_json_from_api("/api/v1/label/host/values?limit=9223372036854775807")
    assert data["data"] == ["server1", "server2"]
    assert "warnings" not in data


@pytest.mark.parametrize("limit", ["9223372036854775808", "18446744073709551615"])
def test_label_values_rejects_limit_above_signed_int64_max(limit):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/label/host/values",
        params={"limit": limit},
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"


@pytest.mark.parametrize(
    "path, expected_values",
    [
        ("/api/v1/label/host/values", ["server1", "server2"]),
        ("/api/v1/label/__name__/values", ["cpu_usage", "memory_usage"]),
    ],
)
def test_label_values_authorizes_through_time_series_table(path, expected_values):
    data = get_json_from_api(path, auth=("metadata_select_time_series", ""))
    assert data["data"] == expected_values


def test_label_values_rejects_outer_time_series_row_policy():
    policy_name = f"prometheus_read_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.prometheus "
        "FOR SELECT USING metric_name = 'cpu_usage' TO metadata_select_time_series"
    )
    try:
        response = requests.get(
            f"http://{node.ip_address}:9093/api/v1/label/host/values",
            auth=("metadata_select_time_series", ""),
        )
        assert response.status_code == 400, response.text
        result = response.json()
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result
        assert "row policies" in result["error"], result
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.prometheus")


def test_label_values_rejects_inner_time_series_row_policy():
    tags_table = node.query("SELECT _table FROM timeSeriesTags(prometheus) LIMIT 1").strip()
    quoted_tags_table = tags_table.replace("`", "``")
    policy_name = f"prometheus_inner_read_policy_{uuid.uuid4().hex}"
    node.query(
        f"CREATE ROW POLICY {policy_name} ON default.`{quoted_tags_table}` "
        "FOR SELECT USING metric_name = 'cpu_usage' TO metadata_select_time_series"
    )
    try:
        response = execute_sql(
            "SELECT count() FROM timeSeriesTags('default', 'prometheus')",
            auth=("metadata_select_time_series", ""),
        )
        assert response.status_code != 200, response.text
        assert "row policies" in response.text, response.text

        response = requests.get(
            f"http://{node.ip_address}:9093/api/v1/label/host/values",
            auth=("metadata_select_time_series", ""),
        )
        assert response.status_code == 400, response.text
        result = response.json()
        assert result["status"] == "error", result
        assert result["errorType"] == "bad_data", result
        assert "row policies" in result["error"], result
    finally:
        node.query(f"DROP ROW POLICY {policy_name} ON default.`{quoted_tags_table}`")


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
    sample_id = uuid.uuid4()
    response = execute_sql(
        "INSERT INTO TABLE FUNCTION timeSeriesSamples('default', 'prometheus') "
        f"SELECT tuple(toUInt64(0), toUUID('{sample_id}')), toDateTime64(2000, 3), 1.0",
        auth=("metadata_insert_temp_table_only", ""),
    )
    assert response.status_code == 200, response.text


def test_label_values_records_query_finish():
    query_id = "prometheus_label_values_query_log_test"
    get_json_from_api("/api/v1/label/host/values", headers={"X-ClickHouse-Query-Id": query_id})
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        "1",
    )


def test_label_values_requires_select_on_configured_time_series_table():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/label/__name__/values",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    result = response.json()
    assert result["status"] == "error", result
    assert result["errorType"] == "bad_data", result
    assert "SELECT" in result["error"], result
    assert "default.prometheus" in result["error"], result


def test_label_values_aggregates_before_returning_rows():
    query_id = "prometheus_label_values_cardinality_query_log_test"
    data = get_json_from_api(
        "/no_filter/api/v1/label/region/values",
        headers={"X-ClickHouse-Query-Id": query_id},
    )
    assert data["data"] == ["eu", "us"]

    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT result_rows FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1",
        "2",
    )
