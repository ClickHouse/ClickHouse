"""A row policy changes no PromQL answer, on one node or through the wrapper, as the selector applies none;
the metadata endpoints refuse a table under a policy or additional_table_filters."""

import contextlib
import json

import pytest
import requests

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import execute_query_via_http_api

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

# The endpoints that derive their answer from the inner tables of a TimeSeries table: the path
# under /api/v1, the name the refusal spells, and the parameters.
METADATA_ENDPOINTS = [
    ("series", "/api/v1/series", {"match[]": "m"}),
    ("labels", "/api/v1/labels", {}),
    ("label/host/values", "/api/v1/label/<name>/values", {}),
    ("metadata", "/api/v1/metadata", {}),
]

FILTERED_USERS = {
    # The short name matches from the default database, the full name from anywhere.
    "prom_filter_short": "{''ts_all'':''metric_name != metric_name''}",
    "prom_filter_full": "{''default.ts_all'':''1''}",
    # Other tables, including this name in another database: not this table.
    "prom_filter_other": "{''ts_other'':''0'',''shard_0.ts_all'':''0''}",
}

# The same series, tags and timestamps as 05055's `m`: `h1` and `h2` hash to one shard and `h3`,
# `h4` to the other, so both jobs of `m` straddle the two shards.
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
        node.query(INSERT_TEST_DATA, settings={"distributed_foreground_insert": 1})
        # The oracle holds exactly what the shards hold, read back through the wrapper.
        node.query(
            "INSERT INTO ts_all (metric_name, tags, time_series) "
            "SELECT metric_name, tags, time_series FROM ts_dist"
        )
        yield cluster
    finally:
        cluster.shutdown()


@contextlib.contextmanager
def restrictive_row_policies():
    """A policy on each table that matches no row of either."""
    node.query(
        "CREATE ROW POLICY p_ts_dist ON ts_dist USING metric_name = 'nothing_matches' TO ALL"
    )
    node.query(
        "CREATE ROW POLICY p_ts_all ON ts_all USING metric_name = 'nothing_matches' TO ALL"
    )
    try:
        yield
    finally:
        node.query("DROP ROW POLICY p_ts_dist ON ts_dist")
        node.query("DROP ROW POLICY p_ts_all ON ts_all")


@contextlib.contextmanager
def filtered_users():
    """Users carrying an additional_table_filters entry, with every grant the endpoints need."""
    for user, filters in FILTERED_USERS.items():
        node.query(
            f"CREATE USER {user} IDENTIFIED WITH no_password "
            f"SETTINGS additional_table_filters = '{filters}'"
        )
    users = ", ".join(FILTERED_USERS)
    node.query(f"GRANT SELECT ON default.* TO {users}")
    node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {users}")
    try:
        yield
    finally:
        node.query(f"DROP USER {users}")


def metadata_response(endpoint, params, user=None):
    if user is not None:
        params = {**params, "user": user, "password": ""}
    return requests.get(
        f"http://{node.ip_address}:9093{LOCAL}/{endpoint}", params=params
    )


def assert_fails_closed(response, endpoint_name, reason):
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["status"] == "error", body
    assert (
        f"The Prometheus {endpoint_name} endpoint is not supported on table"
        in body["error"]
    ), body["error"]
    assert reason in body["error"], body["error"]


def keyed_result(data_json):
    """Keys the series of a query result by their labels: an instant result comes back in no
    defined order, so it has to be keyed before two answers can be compared."""
    data = json.loads(data_json)
    keyed = {
        tuple(sorted(series["metric"].items())): series["value"]
        for series in data["result"]
    }
    assert len(keyed) == len(data["result"]), f"Duplicate label sets in {data_json}"
    return data["resultType"], keyed


def query(handler, promql):
    return keyed_result(
        execute_query_via_http_api(
            node.ip_address, 9093, f"{handler}/query", promql, EVALUATION_TIME
        )
    )


def test_the_row_policies_are_in_force():
    # Without this the test below would pass even if `CREATE ROW POLICY` had done nothing at all.
    assert node.query("SELECT count() FROM ts_dist").strip() != "0"
    assert node.query("SELECT count() FROM ts_all").strip() != "0"
    with restrictive_row_policies():
        assert node.query("SELECT count() FROM ts_dist").strip() == "0"
        assert node.query("SELECT count() FROM ts_all").strip() == "0"


@pytest.mark.parametrize("promql, expected_series", [("m", 4), ("sum by (job) (m)", 2)])
def test_row_policy_leaves_the_promql_answer_unchanged(promql, expected_series):
    unfiltered_dist = query(DIST, promql)
    unfiltered_local = query(LOCAL, promql)
    assert unfiltered_dist == unfiltered_local
    assert len(unfiltered_dist[1]) == expected_series

    with restrictive_row_policies():
        # The policy empties an ordinary SELECT through either table (see the test above), and
        # reaches neither PromQL answer - on the distributed path exactly as on the local one.
        assert query(DIST, promql) == unfiltered_dist
        assert query(LOCAL, promql) == unfiltered_local


@pytest.mark.parametrize("endpoint, endpoint_name, params", METADATA_ENDPOINTS)
def test_metadata_endpoints_fail_closed_under_a_row_policy(
    endpoint, endpoint_name, params
):
    assert metadata_response(endpoint, params).status_code == 200
    with restrictive_row_policies():
        assert_fails_closed(
            metadata_response(endpoint, params),
            endpoint_name,
            "while a row policy applies to it",
        )
    assert metadata_response(endpoint, params).status_code == 200


@pytest.mark.parametrize("endpoint, endpoint_name, params", METADATA_ENDPOINTS)
def test_metadata_endpoints_fail_closed_under_additional_table_filters(
    endpoint, endpoint_name, params
):
    with filtered_users():
        # The filter is in force for these users: an ordinary SELECT sees nothing through it.
        assert (
            node.query("SELECT count() FROM ts_all", user="prom_filter_short").strip()
            == "0"
        )
        for user in ("prom_filter_short", "prom_filter_full"):
            assert_fails_closed(
                metadata_response(endpoint, params, user),
                endpoint_name,
                "with an additional_table_filters entry for it",
            )
        assert (
            metadata_response(endpoint, params, "prom_filter_other").status_code == 200
        )
