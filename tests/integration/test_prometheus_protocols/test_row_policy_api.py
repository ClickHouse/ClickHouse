"""A row policy on the TimeSeries table filters the Prometheus HTTP API and remote read.

The TimeSeries table pushes a translated form of its SELECT row policy into every tags-table read
it generates, so every endpoint which selects time series through that path returns only the series
the policy lets through."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    receive_protobuf_from_remote_read,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

T = 1700000000
TENANT_A = ("tenant_a", "pa")

SERIES_A = [
    {"__name__": "up", "tenant": "a", "instance": "a1"},
    {"__name__": "up", "tenant": "a", "instance": "a2"},
]
SERIES_ALL = SERIES_A + [
    {"__name__": "up", "tenant": "b", "instance": "b1"},
    {"__name__": "secret", "tenant": "b", "secret_only": "yes"},
    {"__name__": "untagged", "instance": "z1"},
]


def get_json_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert (
        response.status_code == 200
    ), f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


def sorted_series(series_list):
    return sorted(series_list, key=lambda labels: sorted(labels.items()))


def metrics_of(result):
    return sorted_series([entry["metric"] for entry in result])


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(
            "INSERT INTO prometheus (metric_name, tags, samples) VALUES"
            f" ('up', {{'tenant': 'a', 'instance': 'a1'}}, [(toDateTime64({T - 60}, 3), 1), (toDateTime64({T}, 3), 1)]),"
            f" ('up', {{'tenant': 'a', 'instance': 'a2'}}, [(toDateTime64({T - 60}, 3), 1), (toDateTime64({T}, 3), 0)]),"
            f" ('up', {{'tenant': 'b', 'instance': 'b1'}}, [(toDateTime64({T - 60}, 3), 1), (toDateTime64({T}, 3), 1)]),"
            f" ('secret', {{'tenant': 'b', 'secret_only': 'yes'}}, [(toDateTime64({T - 60}, 3), 42), (toDateTime64({T}, 3), 43)]),"
            f" ('untagged', {{'instance': 'z1'}}, [(toDateTime64({T - 60}, 3), 7), (toDateTime64({T}, 3), 7)])"
        )
        node.query(
            "INSERT INTO prometheus (metric_family, type, unit, help) VALUES"
            " ('up', 'gauge', '', 'up'), ('secret', 'gauge', '', 'secret'), ('untagged', 'gauge', '', 'untagged')"
        )
        node.query("CREATE USER tenant_a IDENTIFIED BY 'pa'")
        node.query("GRANT SELECT ON default.* TO tenant_a")
        node.query("GRANT CREATE TEMPORARY TABLE ON *.* TO tenant_a")
        node.query(
            "CREATE ROW POLICY tenant_a ON prometheus FOR SELECT USING tags['tenant'] = 'a' TO tenant_a"
        )
        node.query("CREATE ROW POLICY everything ON prometheus FOR SELECT USING 1 TO ALL EXCEPT tenant_a")
        yield cluster
    finally:
        cluster.shutdown()


def test_query_and_query_range():
    all_series = {"query": '{__name__=~".+"}', "time": T}
    result = get_json_from_api("/api/v1/query", params=all_series, auth=TENANT_A)["result"]
    assert metrics_of(result) == sorted_series(SERIES_A)

    result = get_json_from_api(f"/api/v1/query?query=count(up)&time={T}", auth=TENANT_A)["result"]
    assert result == [{"metric": {}, "value": [T, "2"]}]

    result = get_json_from_api(
        f"/api/v1/query_range?query=up&start={T - 60}&end={T}&step=30", auth=TENANT_A
    )["result"]
    assert metrics_of(result) == sorted_series(SERIES_A)

    result = get_json_from_api("/api/v1/query", params=all_series)["result"]
    assert metrics_of(result) == sorted_series(SERIES_ALL)


def test_series_labels_and_label_values():
    series = get_json_from_api(
        "/api/v1/series", params={"match[]": '{__name__=~".+"}', "start": T - 100, "end": T}, auth=TENANT_A
    )
    assert sorted_series(series) == sorted_series(SERIES_A)

    assert get_json_from_api(f"/api/v1/labels?start={T - 100}&end={T}", auth=TENANT_A) == [
        "__name__",
        "instance",
        "tenant",
    ]
    assert get_json_from_api(f"/api/v1/label/tenant/values?start={T - 100}&end={T}", auth=TENANT_A) == ["a"]
    assert get_json_from_api(f"/api/v1/label/__name__/values?start={T - 100}&end={T}", auth=TENANT_A) == ["up"]
    assert get_json_from_api(f"/api/v1/label/instance/values?start={T - 100}&end={T}", auth=TENANT_A) == ["a1", "a2"]

    assert "secret_only" in get_json_from_api(f"/api/v1/labels?start={T - 100}&end={T}")


def test_metadata():
    assert get_json_from_api("/api/v1/metadata", auth=TENANT_A) == {
        "up": [{"type": "gauge", "help": "up", "unit": ""}]
    }
    assert get_json_from_api("/api/v1/metadata?metric=secret", auth=TENANT_A) == {}
    assert sorted(get_json_from_api("/api/v1/metadata")) == ["secret", "untagged", "up"]


def test_metadata_version9_external_tags():
    """A version-9 table with an external tags target still filters /api/v1/metadata."""
    node.query(
        "CREATE TABLE ext_samples_v9_rls (id UUID, timestamp DateTime64(3), value Float64) "
        "ENGINE = MergeTree ORDER BY (id, timestamp)"
    )
    node.query(
        "CREATE TABLE ext_tags_v9_rls (id UUID, metric_name String, tags Map(String, String)) "
        "ENGINE = ReplacingMergeTree ORDER BY (metric_name, id)"
    )
    node.query(
        "CREATE TABLE ext_metrics_v9_rls (metric_family String, type String, unit String, help String) "
        "ENGINE = ReplacingMergeTree ORDER BY metric_family"
    )
    node.query(
        "CREATE TABLE prometheus_v9_rls ENGINE = TimeSeries "
        "SETTINGS version = 9, store_time_ranges = 0, recent_samples_ttl_seconds = 0 "
        "SAMPLES ext_samples_v9_rls TAGS ext_tags_v9_rls METRICS ext_metrics_v9_rls"
    )
    try:
        node.query(
            "INSERT INTO prometheus_v9_rls (metric_name, tags, samples) VALUES"
            f" ('up', {{'tenant': 'a', 'instance': 'a1'}}, [(toDateTime64({T - 60}, 3), 1), (toDateTime64({T}, 3), 1)]),"
            f" ('secret', {{'tenant': 'b', 'secret_only': 'yes'}}, [(toDateTime64({T - 60}, 3), 42), (toDateTime64({T}, 3), 43)])"
        )
        node.query(
            "INSERT INTO prometheus_v9_rls (metric_family, type, unit, help) VALUES"
            " ('up', 'gauge', '', 'up'), ('secret', 'gauge', '', 'secret')"
        )
        node.query(
            "CREATE ROW POLICY tenant_a_v9_rls ON prometheus_v9_rls "
            "FOR SELECT USING tags['tenant'] = 'a' TO tenant_a"
        )
        node.query(
            "CREATE ROW POLICY everything_v9_rls ON prometheus_v9_rls "
            "FOR SELECT USING 1 TO ALL EXCEPT tenant_a"
        )

        assert get_json_from_api("/v9_rls/api/v1/metadata", auth=TENANT_A) == {
            "up": [{"type": "gauge", "help": "up", "unit": ""}]
        }
        assert get_json_from_api("/v9_rls/api/v1/metadata?metric=secret", auth=TENANT_A) == {}
        assert sorted(get_json_from_api("/v9_rls/api/v1/metadata")) == ["secret", "up"]
    finally:
        node.query("DROP ROW POLICY IF EXISTS tenant_a_v9_rls ON prometheus_v9_rls")
        node.query("DROP ROW POLICY IF EXISTS everything_v9_rls ON prometheus_v9_rls")
        node.query("DROP TABLE IF EXISTS prometheus_v9_rls")
        node.query("DROP TABLE IF EXISTS ext_samples_v9_rls")
        node.query("DROP TABLE IF EXISTS ext_tags_v9_rls")
        node.query("DROP TABLE IF EXISTS ext_metrics_v9_rls")


def test_remote_read():
    read_request = convert_read_request_to_protobuf(".+", T - 100, T)
    read_response = receive_protobuf_from_remote_read(
        node.ip_address, 9093, "/read", read_request, auth=TENANT_A
    )
    assert len(read_response.results) == 1
    labels = [
        {label.name: label.value for label in time_series.labels}
        for time_series in read_response.results[0].timeseries
    ]
    assert sorted_series(labels) == sorted_series(SERIES_A)


def test_policy_on_samples_fails_closed():
    node.query("DROP ROW POLICY tenant_a ON prometheus")
    node.query(
        "CREATE ROW POLICY has_samples ON prometheus FOR SELECT USING length(samples) > 0 TO tenant_a"
    )
    try:
        assert node.query("SELECT count() FROM prometheus", user="tenant_a", password="pa") == "5\n"

        response = requests.get(
            f"http://{node.ip_address}:9093/api/v1/query?query=up&time={T}", auth=TENANT_A
        )
        assert response.status_code == 400, response.text
        data = response.json()
        assert data["status"] == "error"
        assert data["errorType"] == "bad_data"
        assert "Unknown expression or function identifier `samples`" in data["error"]
    finally:
        node.query("DROP ROW POLICY has_samples ON prometheus")
        node.query(
            "CREATE ROW POLICY tenant_a ON prometheus FOR SELECT USING tags['tenant'] = 'a' TO tenant_a"
        )
