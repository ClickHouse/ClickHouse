"""Tests that the Prometheus metadata endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) reject a present-but-empty `start=` / `end=` parameter with a
`bad_data` error, like Prometheus does, instead of treating it as an omitted parameter and silently
widening the selected series. An absent parameter still means "no bound"."""

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
    "node_empty_time_params",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


ENDPOINTS = ["/api/v1/series", "/api/v1/labels", "/api/v1/label/host/values"]


def request_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


def base_params():
    """/api/v1/series requires at least one `match[]` selector; add it everywhere for uniformity."""
    return {"match[]": "cpu_usage"}


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        time_series = [({"__name__": "cpu_usage", "host": "server1"}, {1000: 0.5})]
        protobuf = convert_time_series_to_protobuf(time_series)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("param_name", ["start", "end"])
@pytest.mark.parametrize("path", ENDPOINTS)
def test_empty_time_param_is_rejected(path, param_name):
    """`start=` / `end=` (present but empty) must fail with 400 bad_data, not act as omitted."""
    params = base_params()
    params[param_name] = ""
    response = request_api(path, params=params)
    assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"{path}: unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"{path}: unexpected body: {data}"
    assert param_name in data["error"], f"{path}: unexpected error message: {data}"


@pytest.mark.parametrize("path", ENDPOINTS)
def test_absent_time_params_still_mean_no_bound(path):
    """Omitting `start`/`end` entirely keeps working and returns the data."""
    response = request_api(path, params=base_params())
    assert response.status_code == 200, f"{path}: expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"{path}: unexpected body: {data}"
    assert len(data["data"]) > 0, f"{path}: unexpected body: {data}"


@pytest.mark.parametrize("path", ENDPOINTS)
def test_valid_time_params_still_work(path):
    """A valid non-empty range keeps working alongside the empty-value rejection (the test sample is
    written at ~1s of the Unix epoch, so the range [0, 1800000000] covers it)."""
    params = base_params()
    params["start"] = "0"
    params["end"] = "1800000000"
    response = request_api(path, params=params)
    assert response.status_code == 200, f"{path}: expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"{path}: unexpected body: {data}"
    assert len(data["data"]) > 0, f"{path}: unexpected body: {data}"
