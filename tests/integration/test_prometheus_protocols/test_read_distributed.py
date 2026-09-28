"""The HTTP twin of 05055_promql_over_distributed.sql: raw samples are read on every shard and PromQL runs
on the initiator, so every answer must equal a single local table's. The endpoints that cannot merge
shards refuse the wrapper, and a row policy or additional_table_filters entry that a plain SELECT
applies refuses the read instead of being answered past.
"""

import contextlib
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
    get_response_to_http_api_query,
    get_response_to_remote_read,
    keyed_result,
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

# The Distributed wrapper over the two shard-local TimeSeries tables, and the single local
# TimeSeries table holding the union of the same data.
DIST = "/dist/api/v1"
LOCAL = "/local/api/v1"

EVALUATION_TIME = 140

METADATA_HELP = "Metadata of the metric the shards hold"

# The endpoints that derive their answer from the inner tables of a TimeSeries table: the path
# under /api/v1, the name the refusal spells, and the parameters.
METADATA_ENDPOINTS = [
    ("series", "/api/v1/series", {"match[]": "m"}),
    ("labels", "/api/v1/labels", {}),
    ("label/host/values", "/api/v1/label/<name>/values", {}),
    ("metadata", "/api/v1/metadata", {}),
]

# Keyed to the single table and to the wrapper alike; `ts_local` is the wrapper's shard-local table.
FILTERED_USERS = {
    # The short name matches from the default database, the full name from anywhere.
    "prom_filter_short": "{''ts_all'':''metric_name != metric_name'',''ts_dist'':''metric_name != metric_name''}",
    "prom_filter_full": "{''default.ts_all'':''0'',''default.ts_dist'':''0''}",
    # Applied on the shards by a plain SELECT through the wrapper, where the short name is the shard's own.
    "prom_filter_shard_local": "{''ts_local'':''0''}",
    # A literal true restricts nothing, so nothing is refused for it.
    "prom_filter_trivial": "{''ts_all'':''1'',''ts_dist'':''1'',''ts_local'':''1''}",
    # Other tables, including these names in another database: not these tables.
    "prom_filter_other": "{''ts_other'':''0'',''shard_0.ts_all'':''0'',''shard_0.ts_dist'':''0''}",
}
WRAPPER_FILTER_USERS = ["prom_filter_short", "prom_filter_full"]
UNRESTRICTED_FILTER_USERS = ["prom_filter_trivial", "prom_filter_other"]

# The same five series as 05055, sharded on the `host` tag: h1,h2 hash to one shard and h3..h5 to
# the other, so both jobs of `m` straddle the shards and no single shard can answer an aggregation.
INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'host', 'h1'),
        [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 2), (toDateTime64(120, 3), 3),
         (toDateTime64(130, 3), 4), (toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'),
        [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 30),
         (toDateTime64(130, 3), 40), (toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'),
        [(toDateTime64(100, 3), 100), (toDateTime64(110, 3), 200), (toDateTime64(120, 3), 300),
         (toDateTime64(130, 3), 400), (toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'),
        [(toDateTime64(100, 3), 1000), (toDateTime64(110, 3), 2000), (toDateTime64(120, 3), 3000),
         (toDateTime64(130, 3), 4000), (toDateTime64(140, 3), 5000)]),
    ('solo', map('host', 'h5'),
        [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 9),
         (toDateTime64(130, 3), 10), (toDateTime64(140, 3), 11)])
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
            "samples Array(Tuple(DateTime64(0), Float64))) "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        node.query(INSERT_TEST_DATA, settings={"distributed_foreground_insert": 1})
        # The oracle holds exactly what the shards hold, read back through the wrapper.
        node.query(
            "INSERT INTO ts_all (metric_name, tags, samples) "
            "SELECT metric_name, tags, samples FROM ts_dist"
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
        # Keeps `serialize_query_plan` on and forbids a query from changing it, so only a
        # context-level pin can turn it off for the generated read.
        node.query(
            "CREATE USER prom_plan_pinned IDENTIFIED WITH no_password "
            "SETTINGS serialize_query_plan = 1 CONST"
        )
        node.query("GRANT SELECT ON *.* TO prom_plan_pinned")
        node.query("GRANT READ ON REMOTE TO prom_plan_pinned")
        node.query("GRANT CREATE TEMPORARY TABLE ON *.* TO prom_plan_pinned")
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
def shard_local_row_policy():
    """A policy on one shard's table that matches no row of it: a plain SELECT through the wrapper
    then answers from the other shard alone."""
    node.query(
        "CREATE ROW POLICY p_ts_local ON shard_0.ts_local USING metric_name = 'nothing_matches' TO ALL"
    )
    try:
        yield
    finally:
        node.query("DROP ROW POLICY p_ts_local ON shard_0.ts_local")


@contextlib.contextmanager
def filtered_users():
    """Users carrying an additional_table_filters entry, with every grant the reads need."""
    for user, filters in FILTERED_USERS.items():
        node.query(
            f"CREATE USER {user} IDENTIFIED WITH no_password "
            f"SETTINGS additional_table_filters = '{filters}'"
        )
    users = ", ".join(FILTERED_USERS)
    node.query(f"GRANT SELECT ON default.* TO {users}")
    node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {users}")
    node.query(f"GRANT READ ON REMOTE TO {users}")
    try:
        yield
    finally:
        node.query(f"DROP USER {users}")


def as_user(user):
    return {} if user is None else {"user": user, "password": ""}


def metadata_response(endpoint, params, user=None):
    return requests.get(
        f"http://{node.ip_address}:9093{LOCAL}/{endpoint}",
        params={**params, **as_user(user)},
    )


def query_response(handler, promql, user=None):
    return get_response_to_http_api_query(
        node.ip_address,
        9093,
        f"{handler}/query",
        promql,
        EVALUATION_TIME,
        as_user(user),
    )


def query(handler, promql, user=None):
    response = query_response(handler, promql, user)
    assert response.status_code == 200, response.text
    return keyed_result(response.json()["data"])


def range_query(handler, promql, start, end, step):
    return keyed_result(
        json.loads(
            execute_range_query_via_http_api(
                node.ip_address,
                9093,
                f"{handler}/query_range",
                promql,
                start,
                end,
                step,
            )
        )
    )


def values_of(result):
    return {labels: float(value[1]) for labels, value in result[1].items()}


def get_answer(path):
    """The answer of an endpoint over the local TimeSeries table."""
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "success", body
    return body["data"]


def assert_refused(response, *fragments):
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["status"] == "error", body
    for fragment in fragments:
        assert fragment in body["error"], body["error"]


def assert_distributed_refused(path, endpoint):
    """The endpoint refuses the Distributed target: an error naming that endpoint, not a 500
    and not an answer."""
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert_refused(
        response,
        f"The Prometheus {endpoint} endpoint is not supported over a Distributed table",
    )
    assert response.json()["errorType"] == "bad_data", response.text


@pytest.mark.parametrize(
    "params",
    [
        {"serialize_query_plan": 1},
        {"user": "prom_plan_pinned", "password": ""},
    ],
    ids=["asked_for_by_the_request", "const_in_the_profile"],
)
@pytest.mark.parametrize("promql", ["m", "sum by (job) (m)"])
def test_the_read_never_ships_a_plan_to_the_shards(params, promql):
    """A shipped plan is built on the initiator, which would have to resolve the shard-local name in
    its own catalog, where `ts_local` does not exist. The read pins the setting off on its own
    context, so neither a request that asks for it nor a profile that will not let it be changed
    reaches the generated cluster() call."""
    answer = keyed_result(
        json.loads(
            execute_query_via_http_api(
                node.ip_address,
                9093,
                f"{DIST}/query",
                promql,
                EVALUATION_TIME,
                params=params,
            )
        )
    )
    assert answer == query(LOCAL, promql)


@pytest.mark.parametrize(
    "user, settings",
    [("default", {"serialize_query_plan": 1}), ("prom_plan_pinned", {})],
    ids=["asked_for_by_the_query", "const_in_the_profile"],
)
def test_the_table_function_never_ships_a_plan_to_the_shards(user, settings):
    """The same pin on the table-function path."""
    sql = f"SELECT * FROM prometheusQuery({{}}, 'sum by (job) (m)', {EVALUATION_TIME}) ORDER BY ALL"
    distributed = node.query(sql.format("ts_dist"), user=user, settings=settings)
    assert distributed == node.query(sql.format("ts_all"))
    assert len(distributed.splitlines()) == 2, distributed


def test_sharding_key_splits_the_metric_across_both_shards():
    # Without this every aggregation below would be answerable by a single shard on its own,
    # and none of the tests would say anything about the fan-out.
    assert (
        node.query(
            "SELECT tags['job'] AS job, uniqExact(_shard_num) AS shards FROM ts_dist "
            "WHERE metric_name = 'm' GROUP BY job ORDER BY job"
        )
        == "a\t2\nb\t2\n"
    )


@pytest.mark.parametrize(
    "promql, expected_values",
    [
        # All four series of `m`, with their samples at t=140.
        ("m", [5.0, 50.0, 500.0, 5000.0]),
        # Every sample of a series has to reach the same group, whichever shard it came from.
        ("rate(m[40s])", [0.1, 1.0, 10.0, 100.0]),
        # One row per job, each totalling a series taken from each of the two shards.
        ("sum by (job) (m)", [55.0, 5500.0]),
        # A metric only one shard holds.
        ("solo", [11.0]),
    ],
)
def test_instant_query_matches_the_local_table(promql, expected_values):
    distributed = query(DIST, promql)
    assert distributed == query(LOCAL, promql)
    # Not vacuous: the values are those the data gives at t=140.
    assert distributed[0] == "vector"
    assert sorted(values_of(distributed).values()) == pytest.approx(expected_values)


def test_range_query_matches_the_local_table():
    distributed = range_query(DIST, "sum by (job) (m)", 120, EVALUATION_TIME, "10")
    assert distributed == range_query(
        LOCAL, "sum by (job) (m)", 120, EVALUATION_TIME, "10"
    )
    assert distributed[0] == "matrix"
    assert {
        labels: [float(value) for _, value in samples]
        for labels, samples in distributed[1].items()
    } == {
        (("job", "a"),): [33.0, 44.0, 55.0],
        (("job", "b"),): [3300.0, 4400.0, 5500.0],
    }


def test_series_endpoint_refuses_a_distributed_target():
    assert len(get_answer(f"{LOCAL}/series?match[]=m")) == 4
    assert_distributed_refused(f"{DIST}/series?match[]=m", "/api/v1/series")


def test_labels_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/labels") == ["__name__", "host", "job"]
    assert_distributed_refused(f"{DIST}/labels", "/api/v1/labels")


def test_label_values_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/label/host/values") == ["h1", "h2", "h3", "h4", "h5"]
    assert_distributed_refused(
        f"{DIST}/label/host/values", "/api/v1/label/<name>/values"
    )


def test_metadata_endpoint_refuses_a_distributed_target():
    assert get_answer(f"{LOCAL}/metadata") == {
        "m": [{"type": "counter", "help": METADATA_HELP, "unit": ""}]
    }
    assert_distributed_refused(f"{DIST}/metadata", "/api/v1/metadata")


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


def test_the_row_policies_are_in_force():
    # Without this the tests below would pass even if `CREATE ROW POLICY` had done nothing at all.
    assert node.query("SELECT count() FROM ts_dist").strip() != "0"
    assert node.query("SELECT count() FROM ts_all").strip() != "0"
    with restrictive_row_policies():
        # A plain SELECT never answers past the wrapper's policy: refused, as the policy cannot
        # follow the read to the shards, or emptied.
        answer, error = node.query_and_get_answer_with_error(
            "SELECT count() FROM ts_dist"
        )
        assert error or answer.strip() == "0", (answer, error)
        assert node.query("SELECT count() FROM ts_all").strip() == "0"
    with shard_local_row_policy():
        # Applied on that shard alone: the two series the other shard holds still answer.
        assert node.query("SELECT count() FROM ts_dist").strip() == "2"


@pytest.mark.parametrize("promql, expected_series", [("m", 4), ("sum by (job) (m)", 2)])
def test_row_policy_refuses_the_read(promql, expected_series):
    unfiltered_dist = query(DIST, promql)
    unfiltered_local = query(LOCAL, promql)
    assert unfiltered_dist == unfiltered_local
    assert len(unfiltered_dist[1]) == expected_series

    with restrictive_row_policies():
        # The wrapper's policy cannot follow the read to the shards, and the selector on a single
        # table reads the inner tables, which the table's policy does not cover: refused, not answered past.
        assert_refused(
            query_response(DIST, promql),
            "A prometheus query over a Distributed table is not supported on table",
            "while a row policy applies to it",
        )
        assert_refused(
            query_response(LOCAL, promql),
            "A PromQL selector is not supported on table default.ts_all",
            "while a row policy applies to it",
        )
    with shard_local_row_policy():
        # The shard the policy is on refuses its part of the read, and that fails the whole read.
        assert_refused(
            query_response(DIST, promql),
            "A PromQL selector is not supported on table shard_0.ts_local",
            "while a row policy applies to it",
        )
        assert query(LOCAL, promql) == unfiltered_local
    assert query(DIST, promql) == unfiltered_dist
    assert query(LOCAL, promql) == unfiltered_local


def test_additional_table_filters_refuse_the_read():
    unfiltered_dist = query(DIST, "m")
    unfiltered_local = query(LOCAL, "m")
    with filtered_users():
        for user in WRAPPER_FILTER_USERS:
            # The filter is in force: a plain SELECT sees nothing through either table for this user.
            assert (
                node.query("SELECT count() FROM ts_dist", user=user).strip() == "0"
            ), user
            assert (
                node.query("SELECT count() FROM ts_all", user=user).strip() == "0"
            ), user
            assert_refused(
                query_response(DIST, "m", user),
                "A prometheus query over a Distributed table is not supported on table",
                "additional_table_filters entry for it",
            )
            assert_refused(
                query_response(LOCAL, "m", user),
                "A PromQL selector is not supported on table default.ts_all",
                "additional_table_filters entry for it",
            )
        # Keyed to the shard-local table: applied by every shard, so refused by every shard; not this
        # single table.
        assert (
            node.query(
                "SELECT count() FROM ts_dist", user="prom_filter_shard_local"
            ).strip()
            == "0"
        )
        assert_refused(
            query_response(DIST, "m", "prom_filter_shard_local"),
            "A PromQL selector is not supported on table shard_",
            "additional_table_filters entry for it",
        )
        assert query(LOCAL, "m", "prom_filter_shard_local") == unfiltered_local
        for user in UNRESTRICTED_FILTER_USERS:
            assert query(DIST, "m", user) == unfiltered_dist
            assert query(LOCAL, "m", user) == unfiltered_local


@pytest.mark.parametrize("endpoint, endpoint_name, params", METADATA_ENDPOINTS)
def test_metadata_endpoints_fail_closed_under_a_row_policy(
    endpoint, endpoint_name, params
):
    assert metadata_response(endpoint, params).status_code == 200
    with restrictive_row_policies():
        assert_refused(
            metadata_response(endpoint, params),
            f"The Prometheus {endpoint_name} endpoint is not supported on table",
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
        for user in WRAPPER_FILTER_USERS:
            assert_refused(
                metadata_response(endpoint, params, user),
                f"The Prometheus {endpoint_name} endpoint is not supported on table",
                "with an additional_table_filters entry for it",
            )
        # `ts_local` is not this table; a literal true restricts nothing.
        for user in ("prom_filter_shard_local", *UNRESTRICTED_FILTER_USERS):
            assert metadata_response(endpoint, params, user).status_code == 200
