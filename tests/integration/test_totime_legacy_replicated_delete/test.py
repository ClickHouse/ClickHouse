# A standalone `DELETE FROM` on a `Replicated` database is enqueued as query text and replayed by
# the other replica's DDL worker. With the oldest DDL entry format no settings ride along, so a
# legacy `toTime` — including one hidden in a SQL UDF body — must be inlined and canonicalized by
# the initiator (`InterpreterDeleteQuery`), or the replaying replica resolves it with its own
# default and deletes a different row set. The tables are plain MergeTree, so each replica applies
# the mutation to its own copy of the data and the divergence is observable per node.

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_legacy_totime_replicated_delete(started_cluster):
    node1.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica1')"
    )
    node2.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica2')"
    )

    # SQL UDFs are per-server; the replaying replica needs the name resolvable for the raw spelling.
    for node in [node1, node2]:
        node.query("CREATE FUNCTION udf_totime AS x -> toUInt32(toTime(x))")

    node1.query(
        "CREATE TABLE rdb.t (c0 DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()",
        settings={"distributed_ddl_output_mode": "none"},
    )
    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 't'",
        "1",
    )

    # Plain MergeTree data is local to each database replica.
    node1.query("INSERT INTO rdb.t VALUES ('2020-01-02 03:04:05')")
    node2.query("INSERT INTO rdb.t VALUES ('2020-01-02 03:04:05')")

    # Under the legacy setting the predicate matches the row: 86400 + 11045 = 97445.
    node1.query(
        "DELETE FROM rdb.t WHERE udf_totime(c0) = 97445",
        settings={
            "use_legacy_to_time": 1,
            "distributed_ddl_entry_format_version": 1,
            "distributed_ddl_output_mode": "none",
        },
    )

    # The replay lowers to a background mutation, so poll; without the canonicalization the
    # replica resolves the new `toTime` (11045), deletes nothing, and this times out at 1.
    assert_eq_with_retry(node2, "SELECT count() FROM rdb.t", "0")
    assert node1.query("SELECT count() FROM rdb.t").strip() == "0"

    node1.query("DROP DATABASE rdb SYNC")
    node2.query("DROP DATABASE IF EXISTS rdb SYNC")
    for node in [node1, node2]:
        node.query("DROP FUNCTION udf_totime")
