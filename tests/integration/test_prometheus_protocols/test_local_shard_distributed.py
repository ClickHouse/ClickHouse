"""A cluster entry with no <default_database> is local on this node, so the sink writes it and
the read rewrite reads it in-process on the caller's context - which resolves an undeclared
shard-local database to the caller's current one, not to the probe connection's. The probe has
to ask each replica about the table that replica will actually be given."""

import json

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_local_shard.xml",
        "configs/config.d/local_shard_dist.xml",
    ],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/user_default_database.xml",
    ],
)

START_TIME = 1724112000

# The probe's own connection authenticates as `default`, whose current database is `default`;
# this caller's is `metrics`, and that is where its writes and reads land.
CALLER = "?user=prom_metrics&password="
CALLER_PARAMS = {"user": "prom_metrics", "password": ""}

# Callers whose current database is `default`, where `ts_local` is the MergeTree table, each
# missing one grant that the in-process read of the local shard enforces.
NO_TEMP_TABLE_USER = "prom_no_temp_table"
NO_SHARD_SELECT_USER = "prom_no_shard_select"
RESTRICTED_CALLERS = [
    (NO_TEMP_TABLE_USER, "CREATE TEMPORARY TABLE"),
    (NO_SHARD_SELECT_USER, "SELECT ON default.ts_local"),
]

# What the shard probe says about that table, in the words no denied caller may see.
SHARD_LOCAL_LEAK = ["shard-local", "not TimeSeries", "UNEXPECTED_TABLE_ENGINE"]

# Fan-out settings a caller may bring, each over a wrapper they would send over the connection;
# the read pins both the other way.
CALLER_FAN_OUT = [
    ("metrics.prom_local", {"prefer_localhost_replica": 0}),
    ("metrics.prom_two", {"prefer_localhost_replica": 0}),
    ("metrics.prom_two", {"enable_parallel_replicas": 1, "max_parallel_replicas": 2}),
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE metrics")

        node.query("CREATE TABLE metrics.ts_local ENGINE=TimeSeries")
        # Same outer schema, wrong engine, under the name the probe connection resolves.
        node.query(
            "CREATE TABLE default.ts_local AS metrics.ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE metrics.prom_local AS metrics.ts_local "
            "ENGINE = Distributed(local_shard_dist, '', ts_local)"
        )
        node.query(
            "CREATE TABLE metrics.prom_two AS metrics.ts_local "
            "ENGINE = Distributed(local_shard_two_replicas, '', ts_local)"
        )

        # The same pair the other way round: healthy where the probe used to look, wrong engine
        # where the sink actually writes.
        node.query("CREATE TABLE default.ts_swap ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE metrics.ts_swap AS default.ts_swap ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE default.prom_swap AS default.ts_swap "
            "ENGINE = Distributed(local_shard_dist, '', ts_swap)"
        )

        # Both may read the wrapper and call cluster(); each lacks one grant of the local shard's read.
        for user, _ in RESTRICTED_CALLERS:
            node.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
            node.query(f"GRANT READ ON REMOTE TO {user}")
        node.query(f"GRANT SELECT ON *.* TO {NO_TEMP_TABLE_USER}")
        node.query(f"GRANT SELECT ON metrics.prom_local TO {NO_SHARD_SELECT_USER}")
        node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {NO_SHARD_SELECT_USER}")
        yield cluster
    finally:
        cluster.shutdown()


def one_sample(metric_name):
    return convert_time_series_to_protobuf(
        [({"__name__": metric_name, "host": "h0"}, {START_TIME: 1.0})]
    )


def test_remote_write_checks_the_table_the_sink_writes():
    """The engine of `default.ts_local` says nothing about this write: the local shard's rows go
    to `metrics.ts_local`, so that is the table the probe has to verify."""
    send_protobuf_to_remote_write(
        node.ip_address, 9093, f"/local/write{CALLER}", one_sample("local_metric")
    )
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(metrics.ts_local) WHERE metric_name = 'local_metric'",
        "1",
    )
    assert int(node.query("SELECT count() FROM default.ts_local")) == 0


def test_query_reads_the_table_the_rewrite_reads():
    """The read runs the local shard in-process too, so it resolves the same way the write did."""
    result = json.loads(
        execute_query_via_http_api(
            node.ip_address,
            9093,
            "/local_api/query",
            "local_metric",
            START_TIME,
            params=CALLER_PARAMS,
        )
    )["result"]
    assert [sample["value"][1] for sample in result] == ["1"]


def test_remote_write_is_refused_when_only_the_probes_database_is_healthy():
    """`default.ts_swap` is a TimeSeries table of the wrapper's type, and none of the samples
    would have reached it: the sink writes `metrics.ts_swap`, whose engine cannot hold them.
    """
    response = get_response_to_remote_write(
        node.ip_address, 9093, f"/swap/write{CALLER}", one_sample("swap_metric")
    )
    assert response.status_code >= 400
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    assert int(node.query("SELECT count() FROM metrics.ts_swap")) == 0
    assert int(node.query("SELECT count() FROM timeSeriesTags(default.ts_swap)")) == 0


def query_as(endpoint, user, settings=None):
    """The error of the instant or range PromQL endpoint for a caller of this name."""
    params = {"user": user, "password": "", **(settings or {})}
    if endpoint == "query":
        return execute_query_via_http_api(
            node.ip_address,
            9093,
            "/local_api/query",
            "local_metric",
            START_TIME,
            params=params,
            expect_error=True,
        )
    return execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/local_api/query_range",
        "local_metric",
        START_TIME,
        START_TIME + 10,
        "10",
        params=params,
        expect_error=True,
    )


def assert_denied_without_leaking(error, grant):
    assert "Not enough privileges" in error, error
    assert grant in error, error
    for fragment in SHARD_LOCAL_LEAK:
        assert fragment not in error, error


@pytest.mark.parametrize("user, grant", RESTRICTED_CALLERS)
@pytest.mark.parametrize("endpoint", ["query", "query_range"])
def test_query_endpoints_deny_the_local_shard_grants_before_probing(
    endpoint, user, grant
):
    """The local shard is read in-process, so its selector enforces the caller's grants on the table
    it resolves: those are checked before the probe, which would otherwise describe that table.
    """
    # A caller holding every grant is told what the probe found under its own database...
    allowed = query_as(endpoint, "default")
    assert "are not TimeSeries tables" in allowed, allowed

    # ...while one missing a grant the local shard needs later learns only that it has no grant.
    assert_denied_without_leaking(query_as(endpoint, user), grant)


@pytest.mark.parametrize("user, grant", RESTRICTED_CALLERS)
@pytest.mark.parametrize(
    "table_function",
    [
        f"prometheusQuery(metrics.prom_local, 'local_metric', {START_TIME})",
        f"prometheusQueryRange(metrics.prom_local, 'local_metric', {START_TIME}, {START_TIME + 10}, 10)",
    ],
)
def test_table_functions_deny_the_local_shard_grants_before_probing(
    table_function, user, grant
):
    sql = f"SELECT count() FROM {table_function}"
    allowed = node.query_and_get_error(sql)
    assert "are not TimeSeries tables" in allowed, allowed

    # The client prints the server's stack trace after the message; its frames name source files.
    denied = node.query_and_get_error(sql, user=user).split("Stack trace:")[0]
    assert_denied_without_leaking(denied, grant)


@pytest.mark.parametrize(
    "wrapper, settings",
    CALLER_FAN_OUT,
    ids=["no_local_replica", "no_local_replica_of_two", "parallel_replicas"],
)
def test_reads_keep_the_local_shard_in_process_whatever_the_caller_fans_out(
    wrapper, settings
):
    """Read over the connection, the shard would resolve `ts_local` in the pool's database: the
    probe and the read must agree, so a shard that is this server is read here regardless.
    """
    sql = (
        f"SELECT count() FROM prometheusQuery({wrapper}, 'local_metric', {START_TIME})"
    )
    assert node.query(sql, user="prom_metrics", settings=settings).strip() == "1"

    result = json.loads(
        execute_query_via_http_api(
            node.ip_address,
            9093,
            "/local_api/query",
            "local_metric",
            START_TIME,
            params={**CALLER_PARAMS, **settings},
        )
    )["result"]
    assert [sample["value"][1] for sample in result] == ["1"]

    # And the grants of that in-process read are still asked for first.
    denied = query_as("query", NO_SHARD_SELECT_USER, settings)
    assert_denied_without_leaking(denied, "SELECT ON default.ts_local")
