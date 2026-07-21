"""Tests for the `limit` parameter of the Prometheus HTTP API endpoints.

Prometheus defines `limit` on /api/v1/series, /api/v1/labels and /api/v1/label/<name>/values as the
maximum number of returned items, and on /api/v1/query and /api/v1/query_range as the maximum number
of returned series (0 means no limit). It must be handled by the endpoints themselves and not fall
through to ClickHouse's generic `limit` setting: /api/v1/labels prepends the virtual `__name__`
label outside the query, so applying `limit` as a SQL setting would return more items than
requested. When the result is truncated, the response carries the standard Prometheus warning
"results truncated due to limit".
"""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_metadata_limit",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)

# Label names: __name__, datacenter, host, method, status (5 in total).
ALL_LABELS = ["__name__", "datacenter", "host", "method", "status"]
ALL_HOSTS = ["server1", "server2", "server3"]
TRUNCATION_WARNING = "results truncated due to limit"


def send_test_data():
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5},
        ),
        (
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.3},
        ),
        (
            {"__name__": "memory_usage", "host": "server3", "datacenter": "us-east"},
            {1000: 0.8},
        ),
        (
            {
                "__name__": "http_requests_total",
                "host": "server1",
                "method": "GET",
                "status": "200",
            },
            {1000: 100},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_response(path):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url}")
    response = requests.get(url)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


def get_success_json(path):
    response = get_response(path)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    result = response.json()
    assert result["status"] == "success", f"Expected success, got: {result}"
    return result


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_limit_one_returns_exactly_one_item():
    """/api/v1/labels?limit=1 must return exactly one label name (the virtual __name__),
    not `SETTINGS limit = 1` applied to the SQL with __name__ prepended on top."""
    result = get_success_json("/api/v1/labels?limit=1")
    assert result["data"] == ["__name__"]
    assert TRUNCATION_WARNING in result.get("warnings", [])


def test_labels_limit_intermediate():
    result = get_success_json("/api/v1/labels?limit=3")
    assert len(result["data"]) == 3
    assert result["data"] == ALL_LABELS[:3]
    assert TRUNCATION_WARNING in result.get("warnings", [])


def test_labels_limit_larger_than_result_is_not_truncated():
    result = get_success_json("/api/v1/labels?limit=100")
    assert sorted(result["data"]) == ALL_LABELS
    assert "warnings" not in result


def test_labels_limit_zero_means_no_limit():
    result = get_success_json("/api/v1/labels?limit=0")
    assert sorted(result["data"]) == ALL_LABELS
    assert "warnings" not in result


def test_series_limit():
    result = get_success_json("/api/v1/series?limit=2")
    assert len(result["data"]) == 2
    assert TRUNCATION_WARNING in result.get("warnings", [])

    result = get_success_json("/api/v1/series?limit=100")
    assert len(result["data"]) == 4
    assert "warnings" not in result


def test_label_values_limit():
    result = get_success_json("/api/v1/label/host/values?limit=1")
    assert result["data"] == ALL_HOSTS[:1]
    assert TRUNCATION_WARNING in result.get("warnings", [])

    result = get_success_json("/api/v1/label/host/values?limit=100")
    assert result["data"] == ALL_HOSTS
    assert "warnings" not in result


def test_limit_exactly_equal_to_result_size_is_not_truncated():
    result = get_success_json(f"/api/v1/label/host/values?limit={len(ALL_HOSTS)}")
    assert result["data"] == ALL_HOSTS
    assert "warnings" not in result


def test_invalid_limit_is_rejected():
    for invalid in ["-1", "abc", "1.5"]:
        response = get_response(f"/api/v1/labels?limit={invalid}")
        assert response.status_code == 400
        result = response.json()
        assert result["status"] == "error"
        assert "limit" in result["error"]


def test_limit_uint64_max_is_rejected():
    """The endpoints detect truncation by querying `LIMIT limit + 1` rows, so `UInt64` max would wrap
    to zero and turn a valid request into an empty response; it must be rejected instead."""
    uint64_max = 2**64 - 1
    for path in [
        f"/api/v1/series?limit={uint64_max}",
        f"/api/v1/labels?limit={uint64_max}",
        f"/api/v1/label/host/values?limit={uint64_max}",
    ]:
        response = get_response(path)
        assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
        result = response.json()
        assert result["status"] == "error"
        assert "limit" in result["error"]


def test_limit_just_below_uint64_max_is_accepted():
    """The largest representable `limit` value must still behave as an ordinary huge limit."""
    result = get_success_json(f"/api/v1/labels?limit={2**64 - 2}")
    assert sorted(result["data"]) == ALL_LABELS
    assert "warnings" not in result


def test_query_limit():
    """/api/v1/query truncates a vector result to `limit` series (two series match `cpu_usage`)."""
    result = get_success_json("/api/v1/query?query=cpu_usage&time=1000&limit=1")
    assert result["data"]["resultType"] == "vector"
    assert len(result["data"]["result"]) == 1
    assert TRUNCATION_WARNING in result.get("warnings", [])

    for limit in [2, 100, 0]:
        result = get_success_json(f"/api/v1/query?query=cpu_usage&time=1000&limit={limit}")
        assert len(result["data"]["result"]) == 2
        assert "warnings" not in result


def test_query_range_limit():
    """/api/v1/query_range truncates a matrix result to `limit` series."""
    result = get_success_json(
        "/api/v1/query_range?query=cpu_usage&start=1000&end=1030&step=15&limit=1"
    )
    assert result["data"]["resultType"] == "matrix"
    assert len(result["data"]["result"]) == 1
    assert TRUNCATION_WARNING in result.get("warnings", [])

    result = get_success_json(
        "/api/v1/query_range?query=cpu_usage&start=1000&end=1030&step=15&limit=100"
    )
    assert len(result["data"]["result"]) == 2
    assert "warnings" not in result


def test_query_scalar_result_is_not_truncated_by_limit():
    """`limit` is the maximum number of returned series; a scalar result is not a series."""
    result = get_success_json("/api/v1/query?query=23&time=1000&limit=1")
    assert result["data"]["resultType"] == "scalar"
    assert result["data"]["result"] == [1000, "23"]
    assert "warnings" not in result


def test_invalid_limit_is_rejected_on_query_endpoints():
    """The query endpoints validate `limit` the same way as the metadata endpoints."""
    uint64_max = 2**64 - 1
    for path in [
        "/api/v1/query?query=cpu_usage&time=1000&limit=abc",
        f"/api/v1/query?query=cpu_usage&time=1000&limit={uint64_max}",
        "/api/v1/query_range?query=cpu_usage&start=1000&end=1030&step=15&limit=-1",
    ]:
        response = get_response(path)
        assert response.status_code == 400
        result = response.json()
        assert result["status"] == "error"
        assert "limit" in result["error"]
