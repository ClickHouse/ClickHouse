import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
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


def count_on_the_shards(wrapper, metric_name):
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
    time_series = [({"__name__": "bad_metric", "host": "h0"}, {START_TIME: 1.0})]
    protobuf = convert_time_series_to_protobuf(time_series)
    response = get_response_to_remote_write(node.ip_address, 9093, "/bad/write", protobuf)
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
    time_series = [({"__name__": "coarse_metric", "host": "h0"}, {START_TIME: 1.0})]
    protobuf = convert_time_series_to_protobuf(time_series)
    response = get_response_to_remote_write(node.ip_address, 9093, "/coarse/write", protobuf)
    assert response.status_code >= 400
    assert "TYPE_MISMATCH" in response.text
    # The refusal names both types, and nothing was written to either shard.
    assert "Array(Tuple(DateTime64(0), Float64))" in response.text
    assert "Array(Tuple(DateTime64(3), Float64))" in response.text
    assert count_on_the_shards("prom_dist_coarse", "coarse_metric") == 0


def test_remote_write_over_distributed():
    time_series = [
        ({"__name__": "dist_metric", "host": host}, {START_TIME + i: float(i)})
        for i, host in enumerate(HOSTS)
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/dist/write", protobuf)

    # The wrapper inserts asynchronously by default, so the 204 precedes the shard-side landing.
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


def test_remote_write_refuses_insert_shard_id():
    """A plain INSERT with insert_shard_id = 1 sends the batch to shard 1 whatever the key says; the
    endpoint refuses it instead, so a 204 always means the wrapper's own placement."""
    time_series = [
        ({"__name__": "pinned_metric", "host": host}, {START_TIME + i: float(i)})
        for i, host in enumerate(HOSTS)
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    response = get_response_to_remote_write(
        node.ip_address, 9093, "/dist/write?insert_shard_id=1", protobuf
    )
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
        time_series = [
            ({"__name__": "profile_metric", "host": host}, {START_TIME + i: float(i)})
            for i, host in enumerate(HOSTS)
        ]
        protobuf = convert_time_series_to_protobuf(time_series)
        response = get_response_to_remote_write(
            node.ip_address, 9093, "/dist/write?user=prom_pinned&password=", protobuf
        )
        assert response.status_code == 400
        assert "does not accept insert_shard_id" in response.text
        assert count_on_the_shards("prom_dist", "profile_metric") == 0
    finally:
        node.query("DROP USER prom_pinned")


def test_remote_write_refuses_one_random_shard_on_a_keyless_wrapper():
    time_series = [
        ({"__name__": "random_metric", "host": host}, {START_TIME + i: float(i)})
        for i, host in enumerate(HOSTS)
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    # Without a shard choice the sink itself refuses a keyless multi-shard wrapper...
    response = get_response_to_remote_write(
        node.ip_address, 9093, "/keyless/write", protobuf
    )
    assert response.status_code >= 400
    assert "no sharding key provided" in response.text
    # ...and the setting that would let it scatter whole batches over random shards is refused first.
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/keyless/write?insert_distributed_one_random_shard=1",
        protobuf,
    )
    assert response.status_code == 400
    assert "BAD_ARGUMENTS" in response.text
    assert (
        "does not accept insert_shard_id or insert_distributed_one_random_shard"
        in response.text
    )
    assert count_on_the_shards("prom_dist_keyless", "random_metric") == 0
