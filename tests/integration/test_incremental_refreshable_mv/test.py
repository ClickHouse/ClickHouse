import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A Replicated database makes the refreshable MV "coordinated", so its cursor is persisted to the
# CoordinationZnode in Keeper (not just in memory). stay_alive lets us restart the node.
node = cluster.add_instance(
    "node",
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_incremental_rmv_cursor_survives_restart(started_cluster):
    node.query(
        "CREATE DATABASE idb ENGINE = Replicated('/clickhouse/idb/', '{shard}', '{replica}')"
    )
    node.query(
        """
        CREATE TABLE idb.src (k UInt64) ENGINE = ReplicatedMergeTree ORDER BY k
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            add_minmax_index_for_block_number_column = 1,
            add_minmax_index_for_block_offset_column = 1,
            part_minmax_index_columns = 'with_block_number_offset'
        """
    )
    node.query("CREATE TABLE idb.tgt (k UInt64) ENGINE = ReplicatedMergeTree ORDER BY k")
    # REFRESH EVERY 10 YEAR + EMPTY: no automatic refresh; every refresh below is triggered manually.
    node.query(
        """
        CREATE MATERIALIZED VIEW idb.mv
            REFRESH EVERY 10 YEAR SETTINGS refresh_incremental = 1 APPEND
            TO idb.tgt EMPTY
            AS SELECT k FROM idb.src
        """
    )

    # Round 1: commit rows 0..4 and refresh; the advanced cursor is persisted to Keeper.
    node.query("INSERT INTO idb.src SELECT number FROM numbers(5)")
    node.query("SYSTEM REFRESH VIEW idb.mv")
    node.query("SYSTEM WAIT VIEW idb.mv")
    assert node.query("SELECT count(), uniqExact(k) FROM idb.tgt").strip() == "5\t5"

    # Restart: the in-memory RefreshTask state is gone, so only the cursor persisted in Keeper's
    # CoordinationZnode can let the next refresh resume instead of re-reading from the beginning.
    node.restart_clickhouse()
    # Wait for the Replicated database to reload after restart.
    node.query_with_retry("SELECT count() FROM idb.tgt", check_callback=lambda r: r.strip() == "5")

    # Round 2: commit rows 5..9 and refresh. If the cursor survived (Keeper), only the 5 new rows are
    # appended -> 10 rows, 10 distinct. If it had been lost, round 2 would re-read all 10 -> 15 rows.
    node.query("INSERT INTO idb.src SELECT number FROM numbers(5, 5)")
    node.query("SYSTEM REFRESH VIEW idb.mv")
    node.query("SYSTEM WAIT VIEW idb.mv")
    assert node.query("SELECT count(), uniqExact(k) FROM idb.tgt").strip() == "10\t10"

    node.query("DROP DATABASE idb SYNC")
