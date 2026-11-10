"""Tests for how the Prometheus HTTP API reports paths it doesn't serve.

The handler used to resolve the TimeSeries table before working out which endpoint had been
requested, so a path it doesn't serve failed on a table the request never needed. These tests
pin the endpoint down first: an unserved path is reported as not found whether or not a table
is configured, reachable, or supplied, while an endpoint that really does need one keeps
reporting the missing table.
"""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

PROMETHEUS_PORT = 9093

# /no_table is a prefix-routed handler configuring no table, as the documented setup looks
# before a request supplies one. /no_bounds is prefix-routed with a table configured, which is
# deliberately never created here: an unserved path must not depend on it existing either.
NO_TABLE_PREFIX = "/no_table"
UNCREATED_TABLE_PREFIX = "/no_bounds"

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


def api_url(path, prefix=NO_TABLE_PREFIX):
    return f"http://{node.ip_address}:{PROMETHEUS_PORT}{prefix}/api/v1{path}"


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        # cluster.start() waits for the native TCP port only; the Prometheus
        # protocols port can start accepting connections slightly later.
        cluster.wait_for_url(api_url("/status/buildinfo"))
        yield cluster
    finally:
        cluster.shutdown()


def assert_endpoint_not_found(response):
    assert response.status_code == 404, response.text
    body = response.json()
    assert body["status"] == "error", body
    assert body["errorType"] == "not_found", body
    return body["error"]


@pytest.mark.parametrize(
    "path",
    [
        # Status endpoints Prometheus serves but ClickHouse does not. A client probing these
        # used to be told that a TimeSeries table was missing.
        "/status/runtimeinfo",
        "/status/tsdb",
        "/status/flags",
        "/status/config",
        # Not part of the API at all.
        "/no_such_endpoint",
    ],
)
def test_unserved_endpoint_is_not_found_without_a_table(path):
    assert_endpoint_not_found(requests.get(api_url(path)))


def test_unserved_endpoint_does_not_resolve_a_table_even_when_one_is_given():
    # The table parameter names a table that does not exist. The endpoint doesn't need it, so
    # the request must not fail on it.
    response = requests.get(
        api_url("/status/runtimeinfo"), params={"table": "no_such_table"}
    )
    assert_endpoint_not_found(response)


def test_unserved_endpoint_is_not_found_when_the_configured_table_is_missing():
    # This handler configures default.prometheus_no_bounds, which is never created here.
    response = requests.get(
        api_url("/status/runtimeinfo", prefix=UNCREATED_TABLE_PREFIX)
    )
    assert_endpoint_not_found(response)


def test_parse_query_is_reported_as_not_implemented():
    # parse_query only parses the PromQL expression, so like format_query it needs no table.
    error = assert_endpoint_not_found(
        requests.get(api_url("/parse_query"), params={"query": "up"})
    )
    assert "parse_query" in error, error


def test_endpoints_that_need_a_table_still_report_the_missing_table():
    # The fix must not swallow the message that tells the user how to supply a table.
    response = requests.get(api_url("/query"), params={"query": "up"})
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["status"] == "error", body
    assert body["errorType"] == "bad_data", body
    assert "table name is not set" in body["error"], body


@pytest.mark.parametrize(
    "path,query",
    [
        ("/series", {"match[]": "up", "limit": "not_a_number"}),
        ("/labels", {"limit": "not_a_number"}),
        ("/label/job/values", {"limit": "not_a_number"}),
        ("/metadata", {"limit": "not_a_number"}),
        ("/metadata", {"limit_per_metric": "not_a_number"}),
    ],
)
def test_a_missing_table_is_reported_before_the_endpoint_parameters(path, query):
    # These endpoints need a table, so the missing one is reported first: validating their own
    # parameters first would hide the hint about how to supply the table behind a complaint
    # about a parameter the request will not get to use anyway.
    response = requests.get(api_url(path), params=query)
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["errorType"] == "bad_data", body
    assert "table name is not set" in body["error"], body


def test_table_less_endpoints_work_without_a_table():
    buildinfo = requests.get(api_url("/status/buildinfo"))
    assert buildinfo.status_code == 200, buildinfo.text
    assert buildinfo.json()["status"] == "success"

    format_query = requests.get(api_url("/format_query"), params={"query": "foo/bar"})
    assert format_query.status_code == 200, format_query.text
    assert format_query.json()["data"] == "foo / bar"
