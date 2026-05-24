"""Prometheus series/labels API with tags_to_columns (promoted tag columns)."""

import urllib.parse

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def get_json_from_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    response = requests.get(url)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("DROP TABLE IF EXISTS prometheus SYNC")
        node.query(
            "CREATE TABLE prometheus (host_col LowCardinality(String)) "
            "ENGINE=TimeSeries() SETTINGS tags_to_columns = {'host': 'host_col'}"
        )
        time_series = [
            (
                {"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"},
                {1000: 0.5, 1015: 0.6, 1030: 0.7},
            ),
            (
                {"__name__": "memory_usage", "datacenter": "us-west", "host": "server2"},
                {1000: 0.1, 1015: 0.2, 1030: 0.3},
            ),
            # Series with no `host` label -- the promoted column should be empty for these rows,
            # and `/api/v1/labels?match[]={__name__="disk_usage"}` must NOT advertise `host`.
            (
                {"__name__": "disk_usage", "datacenter": "us-south"},
                {1000: 0.9, 1015: 0.91, 1030: 0.92},
            ),
        ]
        protobuf = convert_time_series_to_protobuf(time_series)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_includes_promoted_tag_name():
    data = get_json_from_api("/api/v1/labels")
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_host_from_promoted_column():
    data = get_json_from_api("/api/v1/label/host/values")
    assert "server1" in data


def test_series_includes_host_from_promoted_column():
    data = get_json_from_api("/api/v1/series")
    assert len(data) >= 1
    entry = next(e for e in data if e.get("__name__") == "cpu_usage")
    assert entry.get("host") == "server1"
    assert entry.get("datacenter") == "us-east"


def test_series_multiple_match_union_with_promoted_host_column():
    params = [
        ("match[]", '{__name__="cpu_usage"}'),
        ("match[]", '{__name__="memory_usage"}'),
    ]
    url = f"http://{node.ip_address}:9093/api/v1/series?" + urllib.parse.urlencode(
        params, doseq=True
    )
    response = requests.get(url)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    names = {e["__name__"] for e in data if "__name__" in e}
    assert names == {"cpu_usage", "memory_usage"}


def test_labels_match_zero_rows_returns_empty():
    """`/api/v1/labels?match[]=...` must derive labels from matched rows only:
    when the selector matches no series, the response must be empty and must not
    include `__name__` or any promoted label from `tags_to_columns`."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", '{__name__="does_not_exist"}')],
    )
    assert data == [], data


def test_labels_match_excludes_promoted_label_when_absent_in_matched_rows():
    """Promoted labels must only appear when at least one matched row carries them.
    `disk_usage` was inserted without a `host` label, so its promoted `host_col`
    is empty. Filtering to only `disk_usage` must omit `host` from /labels."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", '{__name__="disk_usage"}')],
    )
    assert "__name__" in data
    assert "datacenter" in data
    assert "host" not in data, data


def test_labels_match_includes_promoted_label_when_present():
    """Sanity check: when matched rows do carry the promoted label, /labels still includes it."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", '{__name__="cpu_usage"}')],
    )
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_promoted_excludes_default_empty():
    """`/api/v1/label/<promoted>/values` must NOT surface `""` for series that simply omit the label.

    `disk_usage` was ingested without `host`, so the promoted `host_col` stores `''` (column default).
    Prometheus treats missing == empty, so the response must include the real `host` values but not `""`.
    """
    data = get_json_from_api("/api/v1/label/host/values")
    assert "" not in data, data
    assert "server1" in data
    assert "server2" in data


def test_label_values_promoted_excludes_default_empty_with_match():
    """Same contract under `match[]`: filtering to the no-host series must yield empty values, not [""]."""
    data = get_json_from_api(
        "/api/v1/label/host/values",
        params=[("match[]", '{__name__="disk_usage"}')],
    )
    assert data == [], data


def test_series_omits_promoted_label_when_absent_in_original_series():
    """`/api/v1/series` must not synthesize promoted labels for series that did not carry them.

    `disk_usage` was ingested without `host`; the promoted `host_col` stores `''` by default.
    Prometheus treats missing == empty, so the series entry must omit the `host` key entirely
    rather than emit `"host": ""`.
    """
    data = get_json_from_api("/api/v1/series")
    disk_entry = next(e for e in data if e.get("__name__") == "disk_usage")
    assert "host" not in disk_entry, disk_entry
    assert disk_entry.get("datacenter") == "us-south"

    cpu_entry = next(e for e in data if e.get("__name__") == "cpu_usage")
    assert cpu_entry.get("host") == "server1"
