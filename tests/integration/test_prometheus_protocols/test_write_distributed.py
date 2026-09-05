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
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

START_TIME = 1724112000
# Eight fixed hosts: the sharding hash is stable, so the split across the two shards is the same
# on every run, and with eight distinct keys both shards receive rows.
HOSTS = [f"h{i}" for i in range(8)]


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


def count_on_the_shards(wrapper, metric_name, flush=True):
    if flush:
        node.query(f"SYSTEM FLUSH DISTRIBUTED {wrapper}")
    return int(
        node.query(
            f"SELECT (SELECT count() FROM timeSeriesTags(shard_0.ts_local) WHERE metric_name = '{metric_name}')"
            f" + (SELECT count() FROM timeSeriesTags(shard_1.ts_local) WHERE metric_name = '{metric_name}')"
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
    assert "TYPE_MISMATCH" in response.text
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
