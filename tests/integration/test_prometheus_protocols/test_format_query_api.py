"""Tests for the Prometheus /api/v1/format_query endpoint."""

import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The endpoint only parses the PromQL expression, so no TimeSeries table is created:
# the tests also verify that the endpoint works without one.
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


def wait_for_prometheus_handlers(timeout=120):
    # cluster.start() waits for the native TCP port, and the Prometheus protocols port
    # can start accepting connections slightly later, so poll it before running the tests.
    deadline = time.monotonic() + timeout
    while True:
        try:
            requests.get(
                f"http://{node.ip_address}:9093/api/v1/format_query",
                params={"query": "up"},
                timeout=5,
            )
            return
        except requests.exceptions.ConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        wait_for_prometheus_handlers()
        yield cluster
    finally:
        cluster.shutdown()


def format_query(query):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": query},
    )
    assert (
        response.status_code == 200
    ), f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # Whitespace is normalized around operators.
        ("foo/bar", "foo / bar"),
        ("  foo   %  bar  ", "foo % bar"),
        # Comments are removed.
        ("# comment\nfoo", "foo"),
        # Redundant parentheses are dropped; necessary ones are kept.
        ("(((foo)))", "foo"),
        ("(foo + bar) * baz", "(foo + bar) * baz"),
        # A __name__ matcher is printed as the metric name.
        ('{__name__="up"}', "up"),
        # The matcher order is preserved.
        ('foo{z="1", a="2"}', 'foo{z="1",a="2"}'),
        # Aggregation and vector-matching modifiers.
        (
            'sum by(job)(rate(http_requests_total{code="200"}[5m]))/2',
            'sum by (job) (rate(http_requests_total{code="200"}[300])) / 2',
        ),
        ("bar + on(a, b) group_left(c) baz", "bar + on(a, b) group_left(c) baz"),
        # Durations are printed as numbers of seconds, and subqueries keep their structure.
        (
            "min_over_time(rate(foo[5m])[30m:5s])",
            "min_over_time(rate(foo[300])[1800:5])",
        ),
        # Numeric literals are canonicalized.
        ("100 * 0x1F", "100 * 31"),
        # @ timestamps are parsed with millisecond precision, like in Prometheus.
        ("foo @ 1.23456789", "foo @ 1.234"),
        ("foo @ 1609746183 offset 5m", "foo @ 1609746183 offset 300"),
    ],
)
def test_format_query(query, expected):
    assert format_query(query) == expected


def test_format_query_post_urlencoded():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        data={"query": "sum   by(job)  (rate(http_requests_total[5m]))"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data == {
        "status": "success",
        "data": "sum by (job) (rate(http_requests_total[300]))",
    }


@pytest.mark.parametrize(
    "query",
    [
        "",  # empty expression
        "foo +",  # incomplete expression
        'foo{bar="unclosed',  # unterminated string
        "foo bar",  # trailing garbage
    ],
)
def test_format_query_rejects_invalid_query(query):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": query},
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error", f"Expected error, got: {data}"
    assert data["errorType"] == "bad_data", f"Expected bad_data, got: {data}"


def test_format_query_missing_query_parameter_is_rejected():
    response = requests.get(f"http://{node.ip_address}:9093/api/v1/format_query")
    assert response.status_code == 400, response.text
    assert response.json()["status"] == "error"
