"""A cluster entry without <default_database> is local, so the sink and the read rewrite run it in-process
on the caller's context, where an undeclared database is the caller's own: the probe must check that table.
"""

import concurrent.futures
import json
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_error_from_query_endpoint,
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
# Pauses a remote write between its shard-target check and its INSERT.
BEFORE_INSERT = "prometheus_remote_write_before_insert"

# A caller whose current database is `default` resolves `ts_local` to the MergeTree table there;
# this caller's is `metrics`, and that is where its writes and reads land.
CALLER = "?user=prom_metrics&password="
CALLER_PARAMS = {"user": "prom_metrics", "password": ""}

# The user of the `local_shard_restricted` cluster entry, granted nothing: a shard that is this
# server itself is checked, written and read in-process on the caller's context, never as them.
CLUSTER_NOBODY = "prom_cluster_nobody"

# Callers whose current database is `default`, where `ts_local` is the MergeTree table. The
# in-process read of the local shard enforces the second grant on either surface; the first is what
# naming a table function that is not readonly costs, and the query endpoints name none.
NO_TEMP_TABLE_USER = "prom_no_temp_table"
NO_SHARD_SELECT_USER = "prom_no_shard_select"
SHARD_SELECT_GRANT = "SELECT ON default.ts_local"
RESTRICTED_CALLERS = [
    (NO_TEMP_TABLE_USER, "CREATE TEMPORARY TABLE"),
    (NO_SHARD_SELECT_USER, SHARD_SELECT_GRANT),
]

# Granted exactly the columns a samples-only remote write names, and its own default database, so
# that it resolves the shard-local table the same way `prom_metrics` does.
COLUMN_INSERT_USER = "prom_column_insert"
SAMPLE_COLUMNS = "metric_name, tags, samples"

# May write the mixed wrapper and nothing else: `metrics.ts_mixed`, the table the local shard of that
# wrapper resolves on the caller's context, is deliberately left ungranted.
MIXED_INSERT_USER = "prom_mixed_insert"
# May write `default.prom_swap`, whose local shard resolves `metrics.ts_swap` on its context: a
# wrong-engine table it is not granted, and so must not be told about.
SWAP_DENIED_USER = "prom_swap_denied"
MIXED_SHARDS = ("metrics.ts_mixed", "remote_shard.ts_mixed")
# Query parameters that make a shard's sink commit a part inside consume() instead of at the end of
# the stream: nothing squashes the pushed block, and a non-zero wait commits it where it lands.
EARLY_COMMIT_SETTING = {
    "input_format_max_block_wait_ms": 1000,
    "min_insert_block_size_rows": 0,
    "min_insert_block_size_bytes": 0,
}
# cityHash64(host) % 2 over two shards of equal weight, shard 0 being the local one.
LOCAL_HOST = "h3"
REMOTE_HOST = "h1"

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

        # The cluster entry names this user, so it exists before the wrapper is first used; it may
        # not even read system.tables, which is what the old probe over its connection selected from.
        node.query(f"CREATE USER {CLUSTER_NOBODY} IDENTIFIED WITH no_password")
        node.query(
            "CREATE TABLE metrics.prom_restricted AS metrics.ts_local "
            "ENGINE = Distributed(local_shard_restricted, '', ts_local)"
        )

        # Both may read the wrapper and call cluster(); each lacks one grant of the local shard's read.
        for user, _ in RESTRICTED_CALLERS:
            node.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
            node.query(f"GRANT READ ON REMOTE TO {user}")
        node.query(f"GRANT SELECT ON *.* TO {NO_TEMP_TABLE_USER}")
        node.query(f"GRANT SELECT ON metrics.prom_local TO {NO_SHARD_SELECT_USER}")
        node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {NO_SHARD_SELECT_USER}")

        node.query(
            f"CREATE USER {COLUMN_INSERT_USER} IDENTIFIED WITH no_password DEFAULT DATABASE metrics"
        )
        for table in ("metrics.prom_local", "metrics.ts_local"):
            node.query(
                f"GRANT INSERT({SAMPLE_COLUMNS}) ON {table} TO {COLUMN_INSERT_USER}"
            )

        # One shard of this wrapper is this server, the other is reached over the connection, so
        # the sharding key decides whether a request touches the local shard at all.
        node.query("CREATE DATABASE remote_shard")
        node.query("CREATE TABLE metrics.ts_mixed ENGINE=TimeSeries")
        node.query("CREATE TABLE remote_shard.ts_mixed ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE metrics.prom_mixed AS metrics.ts_mixed "
            "ENGINE = Distributed(mixed_local_remote_dist, '', ts_mixed, cityHash64(tags['host']))"
        )
        node.query(
            f"CREATE USER {MIXED_INSERT_USER} IDENTIFIED WITH no_password DEFAULT DATABASE metrics"
        )
        node.query(f"GRANT INSERT ON metrics.prom_mixed TO {MIXED_INSERT_USER}")

        node.query(
            f"CREATE USER {SWAP_DENIED_USER} IDENTIFIED WITH no_password DEFAULT DATABASE metrics"
        )
        node.query(f"GRANT INSERT ON default.prom_swap TO {SWAP_DENIED_USER}")
        yield cluster
    finally:
        cluster.shutdown()


def one_sample(metric_name, host="h0"):
    return convert_time_series_to_protobuf(
        [({"__name__": metric_name, "host": host}, {START_TIME: 1.0})]
    )


def mixed_write(metric_name, hosts, user, settings=None):
    """A remote write of one sample per host over the mixed wrapper, as a caller of this name."""
    query = f"user={user}&password="
    for name, value in (settings or {}).items():
        query += f"&{name}={value}"
    return get_response_to_remote_write(
        node.ip_address,
        9093,
        f"/mixed/write?{query}",
        convert_time_series_to_protobuf(
            [
                ({"__name__": metric_name, "host": host}, {START_TIME: 1.0})
                for host in hosts
            ]
        ),
    )


def mixed_counts(metric_name):
    """How many series of this metric each shard of the mixed wrapper holds, local shard first."""
    return [
        int(
            node.query(
                f"SELECT count() FROM timeSeriesTags({table}) WHERE metric_name = '{metric_name}'"
            )
        )
        for table in MIXED_SHARDS
    ]


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


def test_the_local_shard_insert_asks_for_every_column_the_sink_sends():
    """The sink sends every column the wrapper declares, so the shard-local insert asks for INSERT on
    all of them: remote write is refused exactly where a plain INSERT through the wrapper is.
    """
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        f"/local/write?user={COLUMN_INSERT_USER}&password=",
        one_sample("column_grant_metric"),
    )
    assert response.status_code == 403, response.text
    assert "metrics.ts_local" in response.text, response.text

    values = f"('column_grant_metric', map('host', 'h0'), [(toDateTime64({START_TIME}, 3), 1)])"
    sql_error = node.query_and_get_error(
        f"INSERT INTO metrics.prom_local ({SAMPLE_COLUMNS}) VALUES {values}",
        user=COLUMN_INSERT_USER,
        settings={"distributed_foreground_insert": 1},
    )
    assert "metrics.ts_local" in sql_error, sql_error
    assert (
        int(
            node.query(
                "SELECT count() FROM timeSeriesTags(metrics.ts_local) "
                "WHERE metric_name = 'column_grant_metric'"
            )
        )
        == 0
    )


def test_the_mixed_wrapper_sends_each_host_to_the_shard_the_tests_expect():
    """The premise the tests below rest on: one of these hosts is routed to the local shard and the
    other to the remote one, so a request naming only one of them touches only one shard.
    """
    response = mixed_write("routing_metric", [LOCAL_HOST, REMOTE_HOST], "prom_metrics")
    assert response.status_code == 204, response.text
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(metrics.ts_mixed) WHERE metric_name = 'routing_metric'",
        "1",
    )
    assert mixed_counts("routing_metric") == [1, 1]


def test_remote_write_is_accepted_when_no_row_routes_to_the_denied_local_shard():
    """The sink skips a shard whose split is empty, so it never asks for this caller's INSERT on
    `metrics.ts_mixed`: a request whose every row routes to the remote shard is a write it can make.
    """
    ungranted = node.query(
        "CHECK GRANT INSERT ON metrics.ts_mixed", user=MIXED_INSERT_USER
    )
    assert ungranted.strip() == "0", ungranted

    response = mixed_write("remote_only_metric", [REMOTE_HOST], MIXED_INSERT_USER)
    assert response.status_code == 204, response.text
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(remote_shard.ts_mixed) "
        "WHERE metric_name = 'remote_only_metric'",
        "1",
    )
    assert mixed_counts("remote_only_metric") == [0, 1]

    # And a plain INSERT of the same row through the same wrapper, which is the write the remote
    # write models: the two surfaces accept and refuse the same requests from the same caller.
    values = f"('remote_only_sql', map('host', '{REMOTE_HOST}'), [(toDateTime64({START_TIME}, 3), 1)])"
    node.query(
        f"INSERT INTO metrics.prom_mixed ({SAMPLE_COLUMNS}) VALUES {values}",
        user=MIXED_INSERT_USER,
        settings={"distributed_foreground_insert": 1},
    )
    assert mixed_counts("remote_only_sql") == [0, 1]


def test_remote_write_is_still_refused_when_a_row_routes_to_the_local_shard():
    """The same caller, one row further round the hash: this one is delivered in-process on its own
    context, so the insert asks for the grant it has not got and the write is refused with nothing written.
    """
    response = mixed_write("local_routed_metric", [LOCAL_HOST], MIXED_INSERT_USER)
    assert response.status_code == 403, response.text
    assert "Not enough privileges" in response.text, response.text
    assert "metrics.ts_mixed" in response.text, response.text
    assert mixed_counts("local_routed_metric") == [0, 0]


def test_a_batch_that_straddles_the_shards_is_refused_by_the_shard_it_may_not_write():
    """A batch carrying both hosts is refused once the local delivery asks for its grant: accepting
    the half it may write would be a silent partial write under a 204.
    """
    response = mixed_write(
        "straddling_metric", [LOCAL_HOST, REMOTE_HOST], MIXED_INSERT_USER
    )
    assert response.status_code == 403, response.text
    assert "metrics.ts_mixed" in response.text, response.text
    # The refusal is what this pins; what the other shard's inserter had buffered by then is the
    # sink's own business, and it finishes no insert once a job has thrown.
    assert mixed_counts("straddling_metric")[0] == 0


def test_no_shard_keeps_its_half_when_the_caller_asks_for_an_early_commit():
    """The sink forwards the caller's settings to every shard, and these make one commit a part
    inside consume(): the remote half of a refused batch would be visible before the end of the stream.
    """
    response = mixed_write(
        "early_commit_metric",
        [LOCAL_HOST, REMOTE_HOST],
        MIXED_INSERT_USER,
        EARLY_COMMIT_SETTING,
    )
    assert response.status_code == 403, response.text
    assert "metrics.ts_mixed" in response.text, response.text

    # The remote shard commits on a server the refusal never reached, so its rows would appear a
    # moment after the response: the shards have to stay empty, not merely be empty once.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert mixed_counts("early_commit_metric") == [0, 0]
        time.sleep(0.5)


def test_an_early_commit_setting_still_writes_the_shards_a_caller_may_write():
    """Pinned off rather than refused: the same request from a caller granted both shards is the
    write it always was."""
    response = mixed_write(
        "early_commit_ok_metric",
        [LOCAL_HOST, REMOTE_HOST],
        "prom_metrics",
        EARLY_COMMIT_SETTING,
    )
    assert response.status_code == 204, response.text
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(remote_shard.ts_mixed) "
        "WHERE metric_name = 'early_commit_ok_metric'",
        "1",
    )
    assert mixed_counts("early_commit_ok_metric") == [1, 1]


def test_a_denied_caller_is_not_told_what_the_local_shard_holds():
    """The probe reads the shard-local table on the caller's context, so a caller without the grant
    the sink's own insert asks for is skipped there, exactly as a remote replica that denies it is.
    """
    # The premise: a caller holding every grant is told what the probe found on that table.
    allowed = get_response_to_remote_write(
        node.ip_address, 9093, f"/swap/write{CALLER}", one_sample("swap_leak_metric")
    )
    assert "UNEXPECTED_TABLE_ENGINE" in allowed.text, allowed.text

    denied = get_response_to_remote_write(
        node.ip_address,
        9093,
        f"/swap/write?user={SWAP_DENIED_USER}&password=",
        one_sample("swap_leak_metric"),
    )
    assert denied.status_code == 403, denied.text
    assert "Not enough privileges" in denied.text, denied.text
    assert "metrics.ts_swap" in denied.text, denied.text
    for fragment in SHARD_LOCAL_LEAK:
        assert fragment not in denied.text, denied.text


def query_as(endpoint, user, settings=None):
    """The error of the instant or range PromQL endpoint for a caller of this name."""
    params = {"user": user, "password": "", **(settings or {})}
    return get_error_from_query_endpoint(
        node.ip_address,
        9093,
        "/local_api",
        endpoint,
        "local_metric",
        START_TIME,
        START_TIME + 10,
        "10",
        params,
    )


def assert_denied_without_leaking(error, grant):
    assert "Not enough privileges" in error, error
    assert grant in error, error
    for fragment in SHARD_LOCAL_LEAK:
        assert fragment not in error, error


@pytest.mark.parametrize("endpoint", ["query", "query_range"])
def test_query_endpoints_deny_the_local_shard_grant_before_probing(endpoint):
    """The local shard is read in-process, so its selector enforces the caller's grant on the table
    it resolves: it is checked before the probe, which would otherwise describe that table.
    """
    # A caller holding every grant is told what the probe found under its own database...
    allowed = query_as(endpoint, "default")
    assert "are not TimeSeries tables" in allowed, allowed

    # ...while one missing the grant the local shard needs later learns only that it has no grant.
    assert_denied_without_leaking(
        query_as(endpoint, NO_SHARD_SELECT_USER), SHARD_SELECT_GRANT
    )


@pytest.mark.parametrize("endpoint", ["query", "query_range"])
def test_query_endpoints_ask_for_no_temporary_table_grant(endpoint):
    """The endpoints name no table function of their own, and the selector the rewrite names inside
    the cluster() call is readonly: this caller reads the local shard as any other does.
    """
    answered = query_as(endpoint, NO_TEMP_TABLE_USER)
    assert "are not TimeSeries tables" in answered, answered


@pytest.mark.parametrize("user, grant", RESTRICTED_CALLERS)
@pytest.mark.parametrize(
    "table_function",
    [
        f"prometheusQuery(metrics.prom_local, 'local_metric', {START_TIME})",
        f"prometheusQueryRange(metrics.prom_local, 'local_metric', {START_TIME}, {START_TIME + 10}, 10)",
    ],
)
def test_table_functions_deny_their_grants_before_probing(table_function, user, grant):
    """These functions are not readonly, so each caller is denied one grant or the other here."""
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
    assert_denied_without_leaking(denied, SHARD_SELECT_GRANT)


def test_local_shard_is_checked_on_the_callers_context_not_the_cluster_users():
    """A shard that is this server itself is checked, written and read on the caller's context: the
    credentials of its cluster entry play no part, and here they hold no grant at all.
    """
    # The premise: the cluster user is granted nothing, so the table the old probe selected from
    # shows it no row of `metrics` (system.tables is readable by all and filters what it shows).
    hidden = node.query(
        "SELECT count() FROM system.tables WHERE database = 'metrics'",
        user=CLUSTER_NOBODY,
    )
    assert hidden.strip() == "0", hidden

    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        f"/restricted/write{CALLER}",
        one_sample("restricted_metric"),
    )
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(metrics.ts_local) WHERE metric_name = 'restricted_metric'",
        "1",
    )

    result = json.loads(
        execute_query_via_http_api(
            node.ip_address,
            9093,
            "/restricted_api/query",
            "restricted_metric",
            START_TIME,
            params=CALLER_PARAMS,
        )
    )["result"]
    assert [sample["value"][1] for sample in result] == ["1"]

    sql = f"SELECT count() FROM prometheusQuery(metrics.prom_restricted, 'restricted_metric', {START_TIME})"
    assert node.query(sql, user="prom_metrics").strip() == "1"


def test_remote_write_goes_to_the_local_shard_table_it_checked_not_the_name():
    """The check resolves the local shard's table on the caller's context, and the write goes to that
    table: not to whatever the name means by the time the INSERT runs."""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    swapped = False
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {BEFORE_INSERT}")
        pending = pool.submit(
            get_response_to_remote_write,
            node.ip_address,
            9093,
            f"/local/write{CALLER}",
            one_sample("held_metric"),
        )
        node.query(f"SYSTEM WAIT FAILPOINT {BEFORE_INSERT} PAUSE", timeout=60)
        node.query("EXCHANGE TABLES metrics.ts_local AND metrics.ts_swap")
        swapped = True
        node.query(f"SYSTEM NOTIFY FAILPOINT {BEFORE_INSERT}")
        response = pending.result(timeout=60)
        assert response.status_code == 204, response.text
        # The samples are in the TimeSeries table the check saw, now under the other name; the
        # MergeTree table swapped in under the checked name took nothing.
        assert_eq_with_retry(
            node,
            "SELECT count() FROM timeSeriesTags(metrics.ts_swap) WHERE metric_name = 'held_metric'",
            "1",
        )
        assert int(node.query("SELECT count() FROM metrics.ts_local")) == 0
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {BEFORE_INSERT}")
        pool.shutdown(wait=True)
        if swapped:
            node.query("EXCHANGE TABLES metrics.ts_local AND metrics.ts_swap")

    # Under their own names again.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(metrics.ts_local) WHERE metric_name = 'held_metric'",
        "1",
    )
    assert int(node.query("SELECT count() FROM metrics.ts_swap")) == 0
