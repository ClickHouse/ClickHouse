"""Tests for the Prometheus /api/v1/parse_query endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The endpoint only parses PromQL expressions, so no TimeSeries table is created:
# its handler is configured without a table.
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def matcher(name, value, matcher_type="="):
    return {"name": name, "value": value, "type": matcher_type}


def vector_selector(name, matchers, offset=0, timestamp=None, start_or_end=None):
    return {
        "type": "vectorSelector",
        "name": name,
        "matchers": matchers,
        "offset": offset,
        "offsetExpr": None,
        "timestamp": timestamp,
        "startOrEnd": start_or_end,
        "anchored": False,
        "smoothed": False,
    }


def matrix_selector(name, range_ms, matchers, offset=0):
    return {
        "type": "matrixSelector",
        "name": name,
        "range": range_ms,
        "rangeExpr": None,
        "matchers": matchers,
        "offset": offset,
        "offsetExpr": None,
        "timestamp": None,
        "startOrEnd": None,
        "anchored": False,
        "smoothed": False,
    }


def call(name, arg_types, args, variadic=0, return_type="vector"):
    return {
        "type": "call",
        "func": {
            "name": name,
            "argTypes": arg_types,
            "variadic": variadic,
            "returnType": return_type,
        },
        "args": args,
    }


def binary(op, lhs, rhs, matching=None, bool_modifier=False):
    return {
        "type": "binaryExpr",
        "op": op,
        "lhs": lhs,
        "rhs": rhs,
        "matching": matching,
        "bool": bool_modifier,
    }


def vector_matching(card="one-to-one", labels=None, on=False, include=None):
    return {
        "card": card,
        "labels": labels or [],
        "on": on,
        "include": include or [],
        "fillValues": {"lhs": None, "rhs": None},
    }


FOO = vector_selector("foo", [matcher("__name__", "foo")])
BAR = vector_selector("bar", [matcher("__name__", "bar")])


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("1", {"type": "numberLiteral", "val": "1"}),
        # The Prometheus parser folds unary +/- applied to a number literal into the literal.
        ("-1.5", {"type": "numberLiteral", "val": "-1.5"}),
        ("Inf", {"type": "numberLiteral", "val": "+Inf"}),
        # The metric name matcher goes after the explicit matchers, like in Prometheus.
        (
            'up{job="api",status!~"5.."}',
            vector_selector(
                "up",
                [
                    matcher("job", "api"),
                    matcher("status", "5..", "!~"),
                    matcher("__name__", "up"),
                ],
            ),
        ),
        # A selector without a metric name before the braces reports an empty name.
        ('{__name__="up"}', vector_selector("", [matcher("__name__", "up")])),
        (
            "rate(http_requests_total[5m])",
            call(
                "rate",
                ["matrix"],
                [
                    matrix_selector(
                        "http_requests_total",
                        300000,
                        [matcher("__name__", "http_requests_total")],
                    )
                ],
            ),
        ),
        ("foo / bar", binary("/", FOO, BAR, vector_matching())),
        (
            "foo / on(instance) group_left(job) bar",
            binary(
                "/",
                FOO,
                BAR,
                vector_matching("many-to-one", ["instance"], True, ["job"]),
            ),
        ),
        (
            "foo unless bar",
            binary("unless", FOO, BAR, vector_matching("many-to-many")),
        ),
        # The vector matching is null unless both operands are instant vectors.
        ("2 * foo", binary("*", {"type": "numberLiteral", "val": "2"}, FOO)),
        (
            "foo > bool 2",
            binary(
                ">",
                FOO,
                {"type": "numberLiteral", "val": "2"},
                bool_modifier=True,
            ),
        ),
        (
            "sum by (instance) (foo)",
            {
                "type": "aggregation",
                "op": "sum",
                "expr": FOO,
                "param": None,
                "grouping": ["instance"],
                "without": False,
            },
        ),
        (
            "topk(3, foo)",
            {
                "type": "aggregation",
                "op": "topk",
                "expr": FOO,
                "param": {"type": "numberLiteral", "val": "3"},
                "grouping": [],
                "without": False,
            },
        ),
        (
            'count_values("version", foo)',
            {
                "type": "aggregation",
                "op": "count_values",
                "expr": FOO,
                "param": {"type": "stringLiteral", "val": "version"},
                "grouping": [],
                "without": False,
            },
        ),
        (
            "sum without (job) (rate(foo[5m]))[1h:1m]",
            {
                "type": "subquery",
                "expr": {
                    "type": "aggregation",
                    "op": "sum",
                    "expr": call(
                        "rate",
                        ["matrix"],
                        [matrix_selector("foo", 300000, [matcher("__name__", "foo")])],
                    ),
                    "param": None,
                    "grouping": ["job"],
                    "without": True,
                },
                "range": 3600000,
                "rangeExpr": None,
                "step": 60000,
                "stepExpr": None,
                "offset": 0,
                "offsetExpr": None,
                "timestamp": None,
                "startOrEnd": None,
            },
        ),
        # An omitted subquery step is reported as 0.
        (
            "foo[1h:]",
            {
                "type": "subquery",
                "expr": FOO,
                "range": 3600000,
                "rangeExpr": None,
                "step": 0,
                "stepExpr": None,
                "offset": 0,
                "offsetExpr": None,
                "timestamp": None,
                "startOrEnd": None,
            },
        ),
        (
            "foo offset 5m",
            vector_selector("foo", [matcher("__name__", "foo")], offset=300000),
        ),
        (
            "foo[5m] offset -1m",
            matrix_selector("foo", 300000, [matcher("__name__", "foo")], offset=-60000),
        ),
        (
            "foo @ 123",
            vector_selector("foo", [matcher("__name__", "foo")], timestamp=123000),
        ),
        # `@` timestamps are rounded to milliseconds like in the Prometheus parser.
        (
            "foo @ 1.23456789",
            vector_selector("foo", [matcher("__name__", "foo")], timestamp=1235),
        ),
        (
            "foo @ end()",
            vector_selector("foo", [matcher("__name__", "foo")], start_or_end="end"),
        ),
        ("-foo", {"type": "unaryExpr", "op": "-", "expr": FOO}),
        # Functions with optional arguments report their `variadic` count.
        ("day_of_month()", call("day_of_month", ["vector"], [], variadic=1)),
        (
            'label_join(foo, "dst", ",", "a", "b")',
            call(
                "label_join",
                ["vector", "string", "string", "string"],
                [
                    FOO,
                    {"type": "stringLiteral", "val": "dst"},
                    {"type": "stringLiteral", "val": ","},
                    {"type": "stringLiteral", "val": "a"},
                    {"type": "stringLiteral", "val": "b"},
                ],
                variadic=-1,
            ),
        ),
        ("time()", call("time", [], [], return_type="scalar")),
    ],
)
def test_parse_query_returns_prometheus_ast(query, expected):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/parse_query",
        params={"query": query},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"status": "success", "data": expected}


def test_parse_query_escapes_string_values():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/parse_query",
        params={"query": r'foo{job="value\"quoted\\slashed"}'},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "success",
        "data": vector_selector(
            "foo",
            [
                matcher("job", 'value"quoted\\slashed'),
                matcher("__name__", "foo"),
            ],
        ),
    }


def test_parse_query_post_urlencoded():
    query = "# a comment\nsum by (job) (rate(http_requests_total[5m]))"
    url = f"http://{node.ip_address}:9093/api/v1/parse_query"
    post_response = requests.post(
        url,
        data={"query": query},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert post_response.status_code == 200, post_response.text
    get_response = requests.get(url, params={"query": query})
    assert get_response.status_code == 200, get_response.text
    assert post_response.json() == get_response.json()
    assert post_response.json()["status"] == "success"


@pytest.mark.parametrize(
    "query",
    [
        "",
        "foo +",
        "foo{",
        # Wrong number or types of arguments in function calls and aggregations.
        "time(1)",
        "rate(foo)",
        "rate(foo[5m], bar)",
        "sum(foo, bar)",
        "topk(foo, bar)",
        "topk(3)",
        "count_values(1, foo)",
        "label_join(foo)",
        "round(foo, 1, 2)",
        "quantile_over_time(foo[5m], 0.9)",
    ],
)
def test_parse_query_rejects_invalid_query(query):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/parse_query",
        params={"query": query},
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
