import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

cluster = ClickHouseCluster(__file__)

# The up-to-date replica.
node_fresh = cluster.add_instance(
    "node_fresh",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
# The replica whose database defers table loading.
node_lazy = cluster.add_instance(
    "node_lazy",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
# The initiator, which holds no replica of its own.
node_initiator = cluster.add_instance(
    "node_initiator",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_lazy_loaded_replica_is_not_reported_up_to_date(started_cluster):
    # A table of a database with `lazy_load_tables` comes up as a stand-in whose replication threads
    # have not started. The interserver `TablesStatus` handler used to see the stand-in instead of the
    # `ReplicatedMergeTree`, report the replica as not replicated - which the initiator reads as "no
    # delay, up to date" - and the distributed read silently returned the replica's cold local rows.
    node_fresh.query("CREATE DATABASE repl ENGINE = Atomic")
    node_lazy.query("CREATE DATABASE repl ENGINE = Atomic SETTINGS lazy_load_tables = 1")

    for node, replica in ((node_fresh, "fresh"), (node_lazy, "lazy")):
        node.query(
            "CREATE TABLE repl.t (a UInt64) ENGINE = "
            f"ReplicatedMergeTree('/clickhouse/tables/lazy_status_t', '{replica}') ORDER BY a"
        )

    node_initiator.query(
        "CREATE TABLE dist (a UInt64) ENGINE = Distributed('lazy_replica_cluster', 'repl', 't')"
    )

    node_fresh.query("INSERT INTO repl.t SELECT number FROM numbers(1000)")
    node_lazy.query("SYSTEM SYNC REPLICA repl.t", timeout=60)
    assert node_lazy.query("SELECT count() FROM repl.t").strip() == "1000"

    # The lazy replica misses everything inserted while it is down.
    node_lazy.stop_clickhouse()
    node_fresh.query("INSERT INTO repl.t SELECT number FROM numbers(1000, 1000)")

    # After the restart the table is a stand-in again, and nothing accesses it locally.
    node_lazy.start_clickhouse()
    assert (
        node_lazy.query(
            "SELECT engine FROM system.tables WHERE database = 'repl' AND name = 't'"
        ).strip()
        == "TableProxy"
    )

    # Keep it from catching up, so its delay is real for the whole test.
    node_fresh.stop_clickhouse()

    # The staleness gate must see the delay and refuse the replica instead of serving its stale rows.
    with pytest.raises(QueryRuntimeException) as exc:
        node_initiator.query(
            "SELECT count() FROM dist",
            settings={
                "max_replica_delay_for_distributed_queries": 1,
                "fallback_to_stale_replicas_for_distributed_queries": 0,
            },
        )
    assert "ALL_REPLICAS_ARE_STALE" in str(exc.value)

    # With the fallback allowed the query still runs, on the stale replica.
    assert (
        node_initiator.query(
            "SELECT count() FROM dist",
            settings={
                "max_replica_delay_for_distributed_queries": 1,
                "fallback_to_stale_replicas_for_distributed_queries": 1,
            },
        ).strip()
        == "1000"
    )

    node_fresh.start_clickhouse()
    node_lazy.query("SYSTEM SYNC REPLICA repl.t", timeout=60)
    assert node_lazy.query("SELECT count() FROM repl.t").strip() == "2000"

    node_initiator.query("DROP TABLE dist SYNC")
    for node in (node_fresh, node_lazy):
        node.query("DROP DATABASE repl SYNC")
