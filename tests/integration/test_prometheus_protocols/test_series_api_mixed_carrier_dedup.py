"""Tests that `/api/v1/series` deduplicates by the logical Prometheus label set, not by the raw
`tags`-table layout. A supported external tags table can store a `tags_to_columns` tag in two
carriers: the dedicated column and the residual `tags` Map (e.g. legacy rows written before the
dedicated column was adopted). The same logical series stored once per carrier must be returned
once, a single row carrying the same value in both carriers must emit the label key once, and a
row with conflicting values in the two carriers must be rejected with `bad_data`, following the
same normalization rules as `timeSeriesStoreTags` on the write path."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_mixed_carrier_dedup",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def request_api(path, params=None, **kwargs):
    url = f"http://{node.ip_address}:9093{path}"
    response = requests.get(url, params=params, **kwargs)
    return response


def get_series(match):
    response = request_api("/api/v1/series", params={"match[]": match})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp)")
        node.query(
            "CREATE TABLE prometheus_tags (id UUID, metric_name LowCardinality(String),"
            " host LowCardinality(Nullable(String)),"
            " tags Map(LowCardinality(String), String),"
            " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),"
            " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))"
            " ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"
            " SETTINGS allow_dimensions_outside_sorting_key = 1"
        )
        node.query("CREATE TABLE prometheus_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name")
        node.query("CREATE TABLE prometheus ENGINE = TimeSeries SETTINGS tags_to_columns = {'host': 'host'} DATA prometheus_data TAGS prometheus_tags METRICS prometheus_metrics")
        # `cpu_usage{host="server1",instance="i1"}` is stored twice, once per carrier layout:
        # a legacy row with `host` only in the residual Map and a migrated row with `host` only in
        # the dedicated column. `mem_usage` is a single mixed row carrying the same `host` value in
        # both carriers at once. `conflicting_metric` carries two different `host` values. The
        # first row of `late_conflicting_metric` is valid and the second row is malformed, so a
        # finite series limit can use the second row only as a truncation probe.
        node.query(
            "INSERT INTO prometheus_tags VALUES"
            " ('00000000-0000-0000-0000-000000000001', 'cpu_usage', NULL, map('instance', 'i1', 'host', 'server1'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')) ,"
            " ('00000000-0000-0000-0000-000000000002', 'cpu_usage', 'server1', map('instance', 'i1'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')) ,"
            " ('00000000-0000-0000-0000-000000000003', 'mem_usage', 'server2', map('instance', 'i2', 'host', 'server2'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')) ,"
            " ('00000000-0000-0000-0000-000000000004', 'conflicting_metric', 'column_host', map('host', 'map_host'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000005', 'legacy_metric', NULL, map('instance', 'i3', 'host', 'server3'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-00000000000e', 'empty_dedicated_metric', '', map('host', 'server4'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000014', 'missing_label_metric', NULL, map('instance', 'i4'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000007', 'empty_label_name_metric', NULL, map('', 'invalid'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000008', 'empty_label_name_empty_value_metric', NULL, map('', ''),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000009', 'invalid_utf8_metric', NULL, map('bad', unhex('FF')),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000013', unhex('FF'), NULL, map('host', 'invalid_metric_name_host'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-00000000000c', '', NULL, map('host', 'empty_canonical_host'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-00000000000d', '', NULL, map('__name__', 'map_metric', 'host', 'map_only_empty_name'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-00000000000a', 'late_conflicting_metric', 'good_host', map('host', 'good_host'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-00000000000b', 'late_conflicting_metric', 'column_host', map('host', 'map_host'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000006', 'conflicting_metric_name', 'canonical_name', map('__name__', 'map_name'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000015', 'duplicate_map_metric', NULL,"
            "  map('host', 'server5', 'host', 'server6', 'zone', 'a', 'zone', 'b'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000016', 'duplicate_map_metric_name', NULL,"
            "  map('__name__', 'duplicate_map_metric_name', '__name__', 'other_name'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000017', 'duplicate_map_same_value', NULL,"
            "  map('host', 'server7', 'host', 'server7'),"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC'))"
        )
        node.query(
            "INSERT INTO prometheus_data VALUES"
            " ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),"
            " ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2),"
            " ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 3),"
            " ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 4),"
            " ('00000000-0000-0000-0000-000000000005', toDateTime64(1700000000, 3, 'UTC'), 5),"
            " ('00000000-0000-0000-0000-00000000000e', toDateTime64(1700000000, 3, 'UTC'), 6),"
            " ('00000000-0000-0000-0000-000000000014', toDateTime64(1700000000, 3, 'UTC'), 9),"
            " ('00000000-0000-0000-0000-000000000006', toDateTime64(1700000000, 3, 'UTC'), 6),"
            " ('00000000-0000-0000-0000-000000000007', toDateTime64(1700000000, 3, 'UTC'), 7),"
            " ('00000000-0000-0000-0000-000000000008', toDateTime64(1700000000, 3, 'UTC'), 8),"
            " ('00000000-0000-0000-0000-000000000009', toDateTime64(1700000000, 3, 'UTC'), 9),"
            " ('00000000-0000-0000-0000-000000000013', toDateTime64(1700000000, 3, 'UTC'), 13),"
            " ('00000000-0000-0000-0000-000000000010', toDateTime64(1700000000, 3, 'UTC'), 10),"
            " ('00000000-0000-0000-0000-000000000011', toDateTime64(1700000000, 3, 'UTC'), 11),"
            " ('00000000-0000-0000-0000-000000000012', toDateTime64(1700000000, 3, 'UTC'), 12),"
            " ('00000000-0000-0000-0000-000000000015', toDateTime64(1700000000, 3, 'UTC'), 15),"
            " ('00000000-0000-0000-0000-000000000016', toDateTime64(1700000000, 3, 'UTC'), 16),"
            " ('00000000-0000-0000-0000-000000000017', toDateTime64(1700000000, 3, 'UTC'), 17)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_mixed_carrier_rows_collapse_to_one_series():
    """The same logical series stored once per carrier layout is returned exactly once."""
    data = get_series("cpu_usage")
    assert data == [{"__name__": "cpu_usage", "host": "server1", "instance": "i1"}], f"Unexpected series: {data}"


def test_single_row_with_both_carriers_emits_label_once():
    """A row carrying the same value in the Map and in the dedicated column emits `host` once."""
    response = request_api("/api/v1/series", params={"match[]": 'mem_usage{host="server2"}'})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.text.count('"host"') == 1, f"Unexpected body: {response.text}"
    data = response.json()
    assert data["data"] == [{"__name__": "mem_usage", "host": "server2", "instance": "i2"}], f"Unexpected series: {data}"


def test_conflicting_carriers_are_rejected():
    """A row with different `host` values in the Map and in the dedicated column fails with
    `bad_data`, like `timeSeriesStoreTags` on the write path, instead of emitting an invalid
    series with a repeated label key."""
    response = request_api("/api/v1/series", params={"match[]": "conflicting_metric"})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"


@pytest.mark.parametrize("metric_name", ["empty_label_name_metric", "empty_label_name_empty_value_metric"])
def test_empty_label_names_are_rejected(metric_name):
    response = request_api("/api/v1/series", params={"match[]": metric_name})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "empty name" in data["error"], f"Unexpected error message: {data}"


@pytest.mark.parametrize("selector", ['{host="empty_canonical_host"}', '{host="map_only_empty_name"}'])
def test_empty_canonical_metric_names_are_rejected(selector):
    response = request_api("/api/v1/series", params={"match[]": selector})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "empty metric name" in data["error"], f"Unexpected error message: {data}"


def test_series_limit_still_validates_returned_conflicting_row():
    """A conflicting row inside the response limit must still fail closed."""
    response = request_api(
        "/api/v1/series",
        params={"match[]": "late_conflicting_metric", "limit": 2, "max_threads": 1},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"


def test_invalid_utf8_label_values_are_rejected():
    response = request_api("/api/v1/series", params={"match[]": "invalid_utf8_metric"})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "UTF-8" in data["error"], f"Unexpected body: {data}"


def test_label_values_reject_invalid_utf8_values():
    response = request_api(
        "/api/v1/label/bad/values",
        params={"match[]": "invalid_utf8_metric"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "UTF-8" in data["error"], f"Unexpected body: {data}"


def test_label_values_ignores_conflicting_carrier_outside_match():
    response = request_api(
        "/api/v1/label/host/values",
        params={"match[]": '{instance="i1"}'},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.json() == {"status": "success", "data": ["server1"]}, response.text


def test_label_values_rejects_conflicting_carrier_in_selected_series():
    response = request_api(
        "/api/v1/label/host/values",
        params={"match[]": "conflicting_metric"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected body: {data}"


@pytest.mark.parametrize(
    "path, selector, expected_error_fragment",
    [
        ("/api/v1/label/host/values", '{host="empty_canonical_host"}', "empty metric name"),
        ("/api/v1/label/host/values", '{host="map_only_empty_name"}', "empty metric name"),
        ("/api/v1/label/__name__/values", '{host="empty_canonical_host"}', "empty metric name"),
        ("/api/v1/label/host/values", '{host="invalid_metric_name_host"}', "invalid UTF-8"),
    ],
)
def test_label_values_rejects_invalid_canonical_metric_names(path, selector, expected_error_fragment):
    response = request_api(path, params={"match[]": selector})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert expected_error_fragment in data["error"], f"Unexpected body: {data}"


@pytest.mark.parametrize("max_block_size", [1, 100])
def test_late_conflicting_sentinel_is_not_validated(max_block_size):
    """A malformed row fetched only as the limit sentinel must not poison the returned response."""
    response = request_api(
        "/api/v1/series",
        params={
            "match[]": "late_conflicting_metric",
            "limit": 1,
            "max_threads": 1,
            "max_block_size": max_block_size,
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.json() == {
        "status": "success",
        "data": [{"__name__": "late_conflicting_metric", "host": "good_host"}],
        "warnings": ["results truncated due to limit"],
    }


def test_late_emittable_conflict_returns_bad_data_before_response_starts():
    """A malformed returned row must be rejected before a success response is sent."""
    response = request_api(
        "/api/v1/series",
        params={
            "match[]": "late_conflicting_metric",
            "max_threads": 1,
            "max_block_size": 1,
            "http_response_buffer_size": 1,
        },
        stream=True,
    )
    try:
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.json()
        assert data["status"] == "error", f"Unexpected body: {data}"
        assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
        assert "host" in data["error"], f"Unexpected body: {data}"
    finally:
        response.close()


def test_conflicting_metric_name_carriers_are_rejected():
    """The metric name in the Map must agree with the dedicated metric_name column."""
    response = request_api("/api/v1/series", params={"match[]": "conflicting_metric_name"})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "__name__" in data["error"], f"Unexpected error message: {data}"


@pytest.mark.parametrize(
    "selector, expected_error_fragment",
    [
        ('{host="empty_canonical_host"}', "empty metric name"),
        ('{host="map_only_empty_name"}', "empty metric name"),
    ],
)
def test_empty_canonical_metric_name_is_rejected(selector, expected_error_fragment):
    """The canonical metric_name column cannot be replaced by a Map __name__ value."""
    response = request_api("/api/v1/series", params={"match[]": selector})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert expected_error_fragment in data["error"], f"Unexpected body: {data}"


def test_label_values_reject_conflicting_metric_name_carriers():
    """The label-values endpoint must validate the virtual __name__ carrier too."""
    response = request_api(
        "/api/v1/label/__name__/values",
        params={"match[]": "conflicting_metric_name"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "__name__" in data["error"], f"Unexpected error message: {data}"


def test_label_values_rejects_conflicting_duplicate_map_keys():
    response = request_api(
        "/api/v1/label/host/values",
        params={"match[]": "duplicate_map_metric"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"


def test_label_values_rejects_conflicting_duplicate_residual_map_keys():
    response = request_api(
        "/api/v1/label/zone/values",
        params={"match[]": "duplicate_map_metric"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "zone" in data["error"], f"Unexpected error message: {data}"


def test_label_values_rejects_conflicting_duplicate_map_metric_names():
    response = request_api(
        "/api/v1/label/__name__/values",
        params={"match[]": "duplicate_map_metric_name"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "__name__" in data["error"], f"Unexpected error message: {data}"


def test_label_values_accepts_duplicate_map_keys_with_the_same_value():
    response = request_api(
        "/api/v1/label/host/values",
        params={"match[]": "duplicate_map_same_value"},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.json() == {"status": "success", "data": ["server7"]}, response.text


def test_query_reads_legacy_map_carrier_with_short_circuit_disabled():
    response = request_api(
        "/api/v1/query",
        params={
            "query": 'legacy_metric{host="server3"}',
            "time": 1700000000,
            "short_circuit_function_evaluation": "disable",
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert data["data"]["resultType"] == "vector", f"Unexpected body: {data}"
    assert data["data"]["result"][0]["metric"] == {
        "__name__": "legacy_metric",
        "host": "server3",
        "instance": "i3",
    }, f"Unexpected body: {data}"


@pytest.mark.parametrize(
    "matcher, expected_result_count",
    [('host="server4"', 1), ('host!="server4"', 0), ('host=""', 0)],
)
def test_query_preserves_map_fallback_for_empty_dedicated_column(matcher, expected_result_count):
    response = request_api(
        "/api/v1/query",
        params={
            "query": f"empty_dedicated_metric{{{matcher}}}",
            "time": 1700000000,
            "short_circuit_function_evaluation": "disable",
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert len(data["data"]["result"]) == expected_result_count, f"Unexpected body: {data}"


def test_query_preserves_negative_regex_missing_label_semantics():
    response = request_api(
        "/api/v1/query",
        params={
            "query": 'legacy_metric{host!~"other"}',
            "time": 1700000000,
            "short_circuit_function_evaluation": "disable",
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert len(data["data"]["result"]) == 1, f"Unexpected body: {data}"


@pytest.mark.parametrize(
    "matcher, expected_result_count",
    [
        ('host=""', 1),
        ('host!="prod"', 1),
        ('host!=""', 0),
        ('host=~".*"', 1),
        ('host!~"prod"', 1),
    ],
)
def test_query_uses_empty_label_semantics_for_a_missing_label(matcher, expected_result_count):
    response = request_api(
        "/api/v1/query",
        params={
            "query": f"missing_label_metric{{{matcher}}}",
            "time": 1700000000,
            "short_circuit_function_evaluation": "disable",
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    result = data["data"]["result"]
    assert len(result) == expected_result_count, f"Unexpected body: {data}"
    if expected_result_count:
        assert result[0]["metric"] == {
            "__name__": "missing_label_metric",
            "instance": "i4",
        }, f"Unexpected body: {data}"


def test_series_ignores_unrelated_conflicting_carrier_with_short_circuit_disabled():
    response = request_api(
        "/api/v1/series",
        params={
            "match[]": 'legacy_metric{host="server3"}',
            "short_circuit_function_evaluation": "disable",
        },
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert data["data"] == [{"__name__": "legacy_metric", "host": "server3", "instance": "i3"}], data


@pytest.mark.parametrize(
    "selector, expected_data",
    [
        ('empty_dedicated_metric{host="server4"}', [{"__name__": "empty_dedicated_metric", "host": "server4"}]),
        ('empty_dedicated_metric{host!="server4"}', []),
        ('empty_dedicated_metric{host=""}', []),
    ],
)
def test_series_preserves_map_fallback_for_empty_dedicated_column(selector, expected_data):
    response = request_api(
        "/api/v1/series",
        params={"match[]": selector, "short_circuit_function_evaluation": "disable"},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert data["data"] == expected_data, data


def test_series_limit_does_not_hide_conflicting_carrier():
    """A SQL-side limit must not stop validation before a later malformed row."""
    response = request_api(
        "/api/v1/series",
        params={"match[]": '{__name__=~"(aa|bb|cc|conflicting)_metric"}', "limit": "1"},
    )
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"


def test_series_deduplicates_before_limit():
    """Duplicate physical rows must not consume the logical series limit."""
    response = request_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "limit": 1},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert data["data"] == [{"__name__": "cpu_usage", "host": "server1", "instance": "i1"}]
    assert "warnings" not in data, f"Unexpected body: {data}"
