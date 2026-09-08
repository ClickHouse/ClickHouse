import concurrent.futures

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_dist.xml",
        "configs/config.d/two_shards_dist.xml",
        "configs/config.d/two_shards_restricted_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

START_TIME = 1724112000
# A timestamp carrying milliseconds: a shard declaring whole seconds would round it away.
SUB_SECOND_TIME = START_TIME + 0.25
SUB_SECOND_MS = int(round(SUB_SECOND_TIME * 1000))
# Pauses a remote write between its shard-target check and its INSERT.
BEFORE_INSERT = "prometheus_remote_write_before_insert"
# Eight fixed hosts: the sharding hash is stable, so the split across the two shards is the same
# on every run, and with eight distinct keys both shards receive rows.
HOSTS = [f"h{i}" for i in range(8)]

# The user of the `two_shards_restricted` cluster entry: it may select and insert the two shard
# databases, which is all the generated shard read and the shard INSERT ask of it.
CLUSTER_SHARD_USER = "prom_cluster_shard_user"
# What the probe used to select from over that connection, and now does without.
HIDDEN_SYSTEM_TABLES = ["tables", "columns"]

# The user of the `two_shards_column_granted` cluster entry: it holds the shard INSERT column by
# column, which is all a remote write sends, and nothing that lets it read a table's metadata.
CLUSTER_COLUMN_USER = "prom_cluster_column_user"
# The columns a remote write sends, and so the only ones the shard INSERT names.
WRITTEN_COLUMNS = "metric_name, tags, time_series"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE shard_0")
        node.query("CREATE DATABASE shard_1")
        node.query("CREATE TABLE shard_0.ts_local ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_local ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prom_dist AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        # Same outer schema, wrong engine: an ordinary INSERT would accept these rows, and no
        # prometheus read surface could ever return them.
        node.query(
            "CREATE TABLE shard_0.mt_bad AS shard_0.ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE shard_1.mt_bad AS shard_1.ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE prom_dist_bad AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', mt_bad, cityHash64(tags['host']))"
        )
        # The right shards behind a wrapper declaring a coarser `time_series` type than they hold:
        # the sink would round every sample to whole seconds before it reached a shard.
        node.query(
            "CREATE TABLE prom_dist_coarse (metric_name String, tags Map(String, String), "
            "time_series Array(Tuple(DateTime64(0), Float64))) "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        # Two shards and no sharding key: the sink refuses this unless the caller picks a shard.
        node.query(
            "CREATE TABLE prom_dist_keyless AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local)"
        )
        # A TimeSeries table of another `time_series` type, to swap in under a shard-local name:
        # its engine says nothing about the samples the sink would round into it.
        node.query(
            "CREATE TABLE shard_0.ts_coarse (time_series Array(Tuple(DateTime64(0), Float64))) "
            "ENGINE = TimeSeries"
        )

        # The cluster entry names this user, so it exists before the wrapper is first used. Its
        # grants are the databases': the selector and the sink of a TimeSeries table read and write
        # its inner tables by name, on the caller's own context.
        node.query(f"CREATE USER {CLUSTER_SHARD_USER} IDENTIFIED WITH no_password")
        node.query(f"GRANT SELECT, INSERT ON shard_0.* TO {CLUSTER_SHARD_USER}")
        node.query(f"GRANT SELECT, INSERT ON shard_1.* TO {CLUSTER_SHARD_USER}")
        node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {CLUSTER_SHARD_USER}")
        # Its own shard tables, so the exact counts of the other tests are untouched.
        node.query("CREATE TABLE shard_0.ts_restricted ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_restricted ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prom_restricted AS shard_0.ts_restricted "
            "ENGINE = Distributed(two_shards_restricted, '', ts_restricted, cityHash64(tags['host']))"
        )
        # The same credentials over a shard target the probe must still refuse.
        node.query(
            "CREATE TABLE prom_restricted_bad AS shard_0.ts_restricted "
            "ENGINE = Distributed(two_shards_restricted, '', mt_bad, cityHash64(tags['host']))"
        )

        # The other cluster entry names this user, again before the wrapper is first used. Its own
        # shard tables once more, so the exact counts of the other tests are untouched.
        node.query(f"CREATE USER {CLUSTER_COLUMN_USER} IDENTIFIED WITH no_password")
        node.query("CREATE TABLE shard_0.ts_column_granted ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_column_granted ENGINE=TimeSeries")
        for shard_db in ("shard_0", "shard_1"):
            # Only the columns the shard INSERT names, on the table it names: a grant on the table
            # itself would carry SHOW COLUMNS on it and leave nothing for the probe to be denied.
            node.query(
                f"GRANT INSERT({WRITTEN_COLUMNS}) ON {shard_db}.ts_column_granted TO {CLUSTER_COLUMN_USER}"
            )
            # The same on the decoy, so its refusal is the engine check rather than an access error.
            node.query(
                f"GRANT INSERT({WRITTEN_COLUMNS}) ON {shard_db}.mt_bad TO {CLUSTER_COLUMN_USER}"
            )
            # The sink writes the inner tables of a TimeSeries table by name, on the caller's own
            # context: they are named after the outer table's UUID, and hold no outer column.
            ts_uuid = node.query(
                f"SELECT uuid FROM system.tables WHERE database = '{shard_db}' AND name = 'ts_column_granted'"
            ).strip()
            for inner in node.query(
                f"SELECT name FROM system.tables WHERE database = '{shard_db}' AND endsWith(name, '{ts_uuid}')"
            ).split():
                node.query(
                    f"GRANT INSERT ON {shard_db}.`{inner}` TO {CLUSTER_COLUMN_USER}"
                )
        # Declaring only the columns a write sends, so the shard INSERT names no more than those.
        node.query(
            "CREATE TABLE prom_column_granted (metric_name String, tags Map(String, String), "
            "time_series Array(Tuple(DateTime64(3), Float64))) "
            "ENGINE = Distributed(two_shards_column_granted, '', ts_column_granted, cityHash64(tags['host']))"
        )
        # The same columns and the same credentials over a shard target that must still be refused.
        node.query(
            "CREATE TABLE prom_column_granted_bad AS prom_column_granted "
            "ENGINE = Distributed(two_shards_column_granted, '', mt_bad, cityHash64(tags['host']))"
        )
        yield cluster
    finally:
        cluster.shutdown()


def write(path, metric_name, hosts=("h0",)):
    """One sample per host, staggered so the samples of a batch stay distinct."""
    time_series = [
        ({"__name__": metric_name, "host": host}, {START_TIME + i: float(i)})
        for i, host in enumerate(hosts)
    ]
    return get_response_to_remote_write(
        node.ip_address, 9093, path, convert_time_series_to_protobuf(time_series)
    )


def write_one(path, metric_name, host, timestamp):
    """One sample, at a timestamp a shard of a coarser `time_series` type could not hold."""
    return get_response_to_remote_write(
        node.ip_address,
        9093,
        path,
        convert_time_series_to_protobuf(
            [({"__name__": metric_name, "host": host}, {timestamp: 1.0})]
        ),
    )


def count_on_the_shards(wrapper, metric_name, flush=True, table="ts_local"):
    if flush:
        node.query(f"SYSTEM FLUSH DISTRIBUTED {wrapper}")
    return int(
        node.query(
            f"SELECT (SELECT count() FROM timeSeriesTags(shard_0.{table}) WHERE metric_name = '{metric_name}')"
            f" + (SELECT count() FROM timeSeriesTags(shard_1.{table}) WHERE metric_name = '{metric_name}')"
        )
    )


def test_remote_write_rejects_non_timeseries_shards():
    """The wrapper declares no remote database, so each shard resolves `mt_bad` in its own default
    database - the case the initiator cannot answer with its own `currentDatabase()`."""
    response = write("/bad/write", "bad_metric")
    assert response.status_code >= 400
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    # Nothing was written anywhere, on either shard.
    assert (
        node.query(
            "SELECT (SELECT count() FROM shard_0.mt_bad) + (SELECT count() FROM shard_1.mt_bad)"
        ).strip()
        == "0"
    )


def test_remote_write_rejects_a_mismatching_time_series_type():
    response = write("/coarse/write", "coarse_metric")
    assert response.status_code >= 400
    assert "INCOMPATIBLE_SCHEMA" in response.text
    # The refusal names both types, and nothing was written to either shard.
    assert "Array(Tuple(DateTime64(0), Float64))" in response.text
    assert "Array(Tuple(DateTime64(3), Float64))" in response.text
    assert count_on_the_shards("prom_dist_coarse", "coarse_metric") == 0


def test_remote_write_over_distributed():
    response = write("/dist/write", "dist_metric", HOSTS)
    assert response.status_code == 204, response.text
    # Every sample lands exactly once across the shards, and the fixed hash split fills both.
    assert_eq_with_retry(
        node,
        "SELECT (SELECT count() FROM timeSeriesData(shard_0.ts_local))"
        " + (SELECT count() FROM timeSeriesData(shard_1.ts_local))",
        str(len(HOSTS)),
    )
    assert int(node.query("SELECT count() FROM timeSeriesData(shard_0.ts_local)")) > 0
    assert int(node.query("SELECT count() FROM timeSeriesData(shard_1.ts_local)")) > 0

    # Written data reads back through PromQL over the wrapper, in SQL and over HTTP.
    evaluation_time = START_TIME + len(HOSTS)
    sql_result = node.query(
        f"SELECT count() FROM prometheusQuery(prom_dist, 'dist_metric', {evaluation_time})"
    )
    assert int(sql_result) == len(HOSTS)
    http_result = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", "count(dist_metric)", evaluation_time
    )
    assert f'"{len(HOSTS)}"' in http_result


def test_remote_write_over_distributed_ignores_async_insert():
    """A queued batch would be flushed after the shard-target check, into whatever answers to
    the shard-local name by then, so this path always inserts in the foreground."""
    async_inserts_before = int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertQuery'"
        )
    )
    response = write("/dist/write?async_insert=1", "async_dist_metric", HOSTS)
    assert response.status_code == 204, response.text

    # The samples are on the shards, and no asynchronous insert ran: the batch never waited in
    # the queue for a busy timeout before reaching them.
    on_the_shards = count_on_the_shards("prom_dist", "async_dist_metric", flush=False)
    assert on_the_shards == len(HOSTS)
    assert (
        int(
            node.query(
                "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertQuery'"
            )
        )
        == async_inserts_before
    )


def test_remote_write_refuses_insert_shard_id():
    """A plain INSERT with insert_shard_id = 1 sends the batch to shard 1 whatever the key says; the
    endpoint refuses it instead, so a 204 always means the wrapper's own placement."""
    response = write("/dist/write?insert_shard_id=1", "pinned_metric", HOSTS)
    assert response.status_code == 400
    assert "BAD_ARGUMENTS" in response.text
    assert "does not accept insert_shard_id" in response.text
    assert count_on_the_shards("prom_dist", "pinned_metric") == 0


def test_remote_write_refuses_insert_shard_id_from_the_profile():
    """The same refusal when the setting comes from the profile rather than the URL."""
    node.query(
        "CREATE USER prom_pinned IDENTIFIED WITH no_password SETTINGS insert_shard_id = 1"
    )
    node.query("GRANT INSERT ON default.prom_dist TO prom_pinned")
    try:
        response = write(
            "/dist/write?user=prom_pinned&password=", "profile_metric", HOSTS
        )
        assert response.status_code == 400
        assert "does not accept insert_shard_id" in response.text
        assert count_on_the_shards("prom_dist", "profile_metric") == 0
    finally:
        node.query("DROP USER prom_pinned")


def test_remote_write_refuses_one_random_shard_on_a_keyless_wrapper():
    # Without a shard choice the sink itself refuses a keyless multi-shard wrapper...
    response = write("/keyless/write", "random_metric", HOSTS)
    assert response.status_code >= 400
    assert "no sharding key provided" in response.text
    # ...and the setting that would let it scatter whole batches over random shards is refused first.
    response = write(
        "/keyless/write?insert_distributed_one_random_shard=1", "random_metric", HOSTS
    )
    assert response.status_code == 400
    assert "BAD_ARGUMENTS" in response.text
    assert (
        "does not accept insert_shard_id or insert_distributed_one_random_shard"
        in response.text
    )
    assert count_on_the_shards("prom_dist_keyless", "random_metric") == 0


def test_remote_write_refuses_a_shard_target_swapped_after_the_check():
    """A same-schema MergeTree table swapped in under a shard-local name after the check is refused
    by the shard as it would write it: nothing lands, and the write is not acknowledged.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    swapped = False
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {BEFORE_INSERT}")
        # `h3` hashes to shard_0: the whole batch goes to the shard whose target is swapped meanwhile.
        pending = pool.submit(write, "/dist/write", "swapped_metric", ("h3",))
        node.query(f"SYSTEM WAIT FAILPOINT {BEFORE_INSERT} PAUSE", timeout=60)
        node.query("EXCHANGE TABLES shard_0.ts_local AND shard_0.mt_bad")
        swapped = True
        node.query(f"SYSTEM NOTIFY FAILPOINT {BEFORE_INSERT}")
        response = pending.result(timeout=60)
        assert response.status_code >= 500, response.text
        assert "UNEXPECTED_TABLE_ENGINE" in response.text
        # The shard names the engine it found under the name and the one the INSERT expects.
        assert "engine MergeTree" in response.text
        assert "expects TimeSeries" in response.text
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {BEFORE_INSERT}")
        pool.shutdown(wait=True)
        if swapped:
            node.query("EXCHANGE TABLES shard_0.ts_local AND shard_0.mt_bad")

    # The decoy took nothing under the TimeSeries name, nothing reached a TimeSeries table, and the
    # retry lands once the name is right again.
    assert int(node.query("SELECT count() FROM shard_0.mt_bad")) == 0
    assert count_on_the_shards("prom_dist", "swapped_metric") == 0
    assert write("/dist/write", "swapped_metric", ("h3",)).status_code == 204
    assert count_on_the_shards("prom_dist", "swapped_metric") == 1


def test_remote_write_refuses_another_time_series_type_swapped_after_the_check():
    """A TimeSeries table of another `time_series` type swapped in under a shard-local name is refused
    too: its engine passes, and the sink would round every sample into it and answer 204.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    swapped = False
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {BEFORE_INSERT}")
        # `h3` hashes to shard_0: the whole batch goes to the shard whose target is swapped meanwhile.
        pending = pool.submit(
            write_one, "/dist/write", "coarse_swapped_metric", "h3", SUB_SECOND_TIME
        )
        node.query(f"SYSTEM WAIT FAILPOINT {BEFORE_INSERT} PAUSE", timeout=60)
        node.query("EXCHANGE TABLES shard_0.ts_local AND shard_0.ts_coarse")
        swapped = True
        node.query(f"SYSTEM NOTIFY FAILPOINT {BEFORE_INSERT}")
        response = pending.result(timeout=60)
        # A 4xx would have Prometheus drop the batch; this refusal is the retryable kind.
        assert response.status_code >= 500, response.text
        assert "INCOMPATIBLE_SCHEMA" in response.text
        # The shard names the type it declares under the name and the one the INSERT expects.
        assert "Array(Tuple(DateTime64(0), Float64))" in response.text
        assert "Array(Tuple(DateTime64(3), Float64))" in response.text
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {BEFORE_INSERT}")
        pool.shutdown(wait=True)
        if swapped:
            node.query("EXCHANGE TABLES shard_0.ts_local AND shard_0.ts_coarse")

    # The decoy took no rounded sample under the TimeSeries name, nothing reached a shard of the
    # wrapper's own type, and the retry keeps the milliseconds the decoy would have dropped.
    assert (
        node.query(
            "SELECT (SELECT count() FROM timeSeriesData(shard_0.ts_coarse))"
            " + (SELECT count() FROM timeSeriesTags(shard_0.ts_coarse))"
        ).strip()
        == "0"
    )
    assert count_on_the_shards("prom_dist", "coarse_swapped_metric") == 0
    assert (
        write_one(
            "/dist/write", "coarse_swapped_metric", "h3", SUB_SECOND_TIME
        ).status_code
        == 204
    )
    assert count_on_the_shards("prom_dist", "coarse_swapped_metric") == 1
    assert (
        node.query(
            "SELECT count() FROM timeSeriesData(shard_0.ts_local) "
            f"WHERE toUnixTimestamp64Milli(timestamp) = {SUB_SECOND_MS}"
        ).strip()
        == "1"
    )


def test_the_probe_asks_the_cluster_user_no_more_than_the_shards_do():
    """The shard-target check runs over the cluster's own connection, so it may ask that user for
    nothing the generated shard read and the shard INSERT do: here it may not read `system` at all.
    """
    try:
        for system_table in HIDDEN_SYSTEM_TABLES:
            # Permissive for everyone else first, so only this user's view of the table is emptied.
            node.query(
                f"CREATE ROW POLICY p_rest_{system_table} ON system.{system_table} "
                f"USING 1 TO ALL EXCEPT {CLUSTER_SHARD_USER}"
            )
            node.query(
                f"CREATE ROW POLICY p_hide_{system_table} ON system.{system_table} "
                f"USING 0 TO {CLUSTER_SHARD_USER}"
            )
        # The premise: as the cluster user the real shard read and the real shard INSERT both run...
        node.query(
            f"SELECT count() FROM timeSeriesSelector(shard_0.ts_restricted, 'premise_metric', 0, {START_TIME})",
            user=CLUSTER_SHARD_USER,
        )
        node.query(
            "INSERT INTO shard_0.ts_restricted (metric_name, tags, time_series) VALUES "
            f"('premise_metric', map('host', 'h3'), [(toDateTime64({START_TIME}, 3), 1)])",
            user=CLUSTER_SHARD_USER,
        )
        # ...while the tables the probe used to select from hold no row it may see.
        for system_table in HIDDEN_SYSTEM_TABLES:
            hidden = node.query(
                f"SELECT count() FROM system.{system_table}", user=CLUSTER_SHARD_USER
            )
            assert hidden.strip() == "0", hidden

        # A remote write through the wrapper is acknowledged, and every sample is on the shards.
        response = write("/restricted/write", "restricted_metric", HOSTS)
        assert response.status_code == 204, response.text
        assert count_on_the_shards(
            "prom_restricted", "restricted_metric", table="ts_restricted"
        ) == len(HOSTS)

        # And a PromQL read over the same connection answers from the shards it just wrote.
        evaluation_time = START_TIME + len(HOSTS)
        sql_result = node.query(
            f"SELECT count() FROM prometheusQuery(prom_restricted, 'restricted_metric', {evaluation_time})"
        )
        assert int(sql_result) == len(HOSTS)

        # Not vacuous: over the very same credentials the probe still refuses a shard target it must.
        engine_error = node.query_and_get_error(
            f"SELECT count() FROM prometheusQuery(prom_restricted_bad, 'restricted_metric', {evaluation_time})"
        )
        assert "are not TimeSeries tables" in engine_error, engine_error
    finally:
        for system_table in HIDDEN_SYSTEM_TABLES:
            node.query(
                f"DROP ROW POLICY IF EXISTS p_hide_{system_table} ON system.{system_table}"
            )
            node.query(
                f"DROP ROW POLICY IF EXISTS p_rest_{system_table} ON system.{system_table}"
            )


def test_the_probe_accepts_a_cluster_user_granted_only_the_written_columns():
    """The narrower sibling: a column-level INSERT is all the shard write asks for, and it carries no
    right to read metadata, so the shard-target check may not ask that user for it either.
    """
    # The premise: as the cluster user the real shard INSERT runs...
    node.query(
        f"INSERT INTO shard_0.ts_column_granted ({WRITTEN_COLUMNS}) VALUES "
        f"('premise_metric', map('host', 'h3'), [(toDateTime64({START_TIME}, 3), 1)])",
        user=CLUSTER_COLUMN_USER,
    )
    # ...while both statements the probe makes on a remote replica are denied it.
    for statement in ("SHOW CREATE TABLE", "DESC TABLE"):
        denied = node.query_and_get_error(
            f"{statement} shard_0.ts_column_granted", user=CLUSTER_COLUMN_USER
        )
        assert "Not enough privileges" in denied, denied

    # A remote write through the wrapper is acknowledged, and every sample is on the shards.
    response = write("/column_granted/write", "column_granted_metric", HOSTS)
    assert response.status_code == 204, response.text
    assert count_on_the_shards(
        "prom_column_granted", "column_granted_metric", table="ts_column_granted"
    ) == len(HOSTS)

    # Not vacuous: over the very same credentials a shard target that must be refused still is - by
    # the shard's own insert, which names the engine it found and the one the INSERT expects.
    response = write("/column_granted_bad/write", "column_granted_metric", HOSTS)
    assert response.status_code >= 500, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    assert "engine MergeTree" in response.text
    assert "expects TimeSeries" in response.text
    # And the decoy took nothing, on either shard.
    assert (
        node.query(
            "SELECT (SELECT count() FROM shard_0.mt_bad) + (SELECT count() FROM shard_1.mt_bad)"
        ).strip()
        == "0"
    )
