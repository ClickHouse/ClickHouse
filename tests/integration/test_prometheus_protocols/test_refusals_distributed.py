"""The inner-table readers (/api/v1/series, /labels, /label/<name>/values, /metadata) and remote read
with its node-local counter cannot merge shards, so each refuses a Distributed target.
"""

import json

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_metrics_metadata_to_protobuf,
    convert_read_request_to_protobuf,
    error_code,
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    get_response_to_remote_read,
    receive_protobuf_from_remote_read,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_distributed.xml",
        "configs/config.d/two_shards_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

DIST = "/dist/api/v1"
LOCAL = "/local/api/v1"

EVALUATION_TIME = 140

METADATA_HELP = "Metadata of the metric the shards hold"

# The same series, tags and timestamps as 05055's `m`: `h1` and `h2` hash to one shard and `h3`,
# `h4` to the other, so the Distributed target really does span both shards.
INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, time_series) VALUES
    ('m', map('job', 'a', 'host', 'h1'),
        [(toDateTime64(100, 3), 1), (toDateTime64(120, 3), 3), (toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'),
        [(toDateTime64(100, 3), 10), (toDateTime64(120, 3), 30), (toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'),
        [(toDateTime64(100, 3), 100), (toDateTime64(120, 3), 300), (toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'),
        [(toDateTime64(100, 3), 1000), (toDateTime64(120, 3), 3000), (toDateTime64(140, 3), 5000)])
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE shard_0")
        node.query("CREATE DATABASE shard_1")
        node.query("CREATE TABLE shard_0.ts_local ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_local ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE ts_dist AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        node.query("CREATE TABLE ts_all ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE ts_coarse (metric_name String, tags Map(String, String), "
            "time_series Array(Tuple(DateTime64(0), Float64))) "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        node.query(INSERT_TEST_DATA, settings={"distributed_foreground_insert": 1})
        node.query(
            "INSERT INTO ts_all (metric_name, tags, time_series) "
            "SELECT metric_name, tags, time_series FROM ts_dist"
        )
        # Metrics metadata only reaches a table through the remote write protocol.
        send_protobuf_to_remote_write(
            node.ip_address,
            9093,
            f"{LOCAL}/write",
            convert_metrics_metadata_to_protobuf([("m", "COUNTER", METADATA_HELP, "")]),
        )
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesMetrics(ts_all)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def get_answer(path):
    """The answer of an endpoint over the local TimeSeries table."""
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "success", body
    return body["data"]


def assert_refused(path, endpoint):
    """The endpoint refuses the Distributed target: an error naming that endpoint, not a 500
    and not an answer."""
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["status"] == "error", body
    assert body["errorType"] == "bad_data", body
    assert (
        f"The Prometheus {endpoint} endpoint is not supported over a Distributed table"
        in body["error"]
    ), body["error"]


def test_series_endpoint_refuses_a_distributed_target():
    assert len(get_answer(f"{LOCAL}/series?match[]=m")) == 4
    assert_refused(f"{DIST}/series?match[]=m", "/api/v1/series")


def test_labels_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/labels") == ["__name__", "host", "job"]
    assert_refused(f"{DIST}/labels", "/api/v1/labels")


def test_label_values_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/label/host/values") == ["h1", "h2", "h3", "h4"]
    assert_refused(f"{DIST}/label/host/values", "/api/v1/label/<name>/values")


def test_metadata_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/metadata") == {
        "m": [{"type": "counter", "help": METADATA_HELP, "unit": ""}]
    }
    assert_refused(f"{DIST}/metadata", "/api/v1/metadata")


def test_remote_read_refuses_a_distributed_target():
    read_request = convert_read_request_to_protobuf("^m$", 0, EVALUATION_TIME)

    local = receive_protobuf_from_remote_read(
        node.ip_address, 9093, f"{LOCAL}/read", read_request
    )
    assert [
        label.value
        for result in local.results
        for series in result.timeseries
        for label in series.labels
        if label.name == "__name__"
    ] == ["m"] * 4

    response = get_response_to_remote_read(
        node.ip_address, 9093, f"{DIST}/read", read_request
    )
    # Remote read reports the error code itself, so this pins the code and not its wording.
    assert response.headers["X-ClickHouse-Exception-Code"] == error_code(
        node, "NOT_IMPLEMENTED"
    )
    assert response.status_code == requests.codes.not_implemented, response.text
    assert "NOT_IMPLEMENTED" in response.text


def test_the_query_endpoints_still_answer_over_a_distributed_target():
    # The refusals above are specific to the endpoints that cannot merge across shards; PromQL
    # itself keeps working over the very same target, and still sees all four series.
    instant = json.loads(
        execute_query_via_http_api(
            node.ip_address, 9093, f"{DIST}/query", "m", EVALUATION_TIME
        )
    )
    assert len(instant["result"]) == 4, instant

    ranged = json.loads(
        execute_range_query_via_http_api(
            node.ip_address,
            9093,
            f"{DIST}/query_range",
            "m",
            100,
            EVALUATION_TIME,
            "20",
        )
    )
    assert len(ranged["result"]) == 4, ranged


def test_the_query_endpoints_refuse_a_wrapper_of_another_time_series_type():
    # Legal for Distributed, which never validates the shard-side structure, but PromQL would parse
    # the times with the wrapper's scale and read with the shards': refused, and both types named.
    response = requests.get(
        f"http://{node.ip_address}:9093/coarse/api/v1/query?query=m&time={EVALUATION_TIME}"
    )
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["status"] == "error", body
    assert "Array(Tuple(DateTime64(0), Float64))" in body["error"], body["error"]
    assert "Array(Tuple(DateTime64(3), Float64))" in body["error"], body["error"]
