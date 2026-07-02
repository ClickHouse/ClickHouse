# Regression test for the single-stream fallback of parallel plain INSERTs with dependent
# materialized views under the legacy deduplication hash modes.
#
# max_insert_threads parallelizes the writing side of a plain INSERT by resizing the pipeline to
# several sink streams. When parallel_view_processing is enabled this also fans out the dependent
# materialized-view chains, and each branch gets its own per-stream UpdateDeduplicationInfoWithViewIDTransform
# whose view block number restarts from zero. Under the legacy hash modes (old_separate_hashes /
# compatible_double_hashes) the VIEW-level block id is hash(data_hash : view_id : view_block_number)
# and drops the (global) source block number, so two identical source blocks landing on different
# branches produce the same materialized-view block id and one of them is skipped as a duplicate -
# silently dropping rows of a single INSERT. Such inserts must therefore keep the dependent-MV
# deduplication path single-stream. Under new_unified_hash the VIEW id also folds in the global source
# block number, so the branches stay distinct and the fan-out is safe.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NODES = {
    "node_old": cluster.add_instance(
        "node_old", main_configs=["configs/dedup_old.xml"]
    ),
    "node_compatible": cluster.add_instance(
        "node_compatible", main_configs=["configs/dedup_compatible.xml"]
    ),
    "node_new": cluster.add_instance(
        "node_new", main_configs=["configs/dedup_new.xml"]
    ),
}

# Number of parallel insert streams requested.
THREADS = 4
ROWS = 100000

# Settings shared by both the EXPLAIN PIPELINE topology check and the actual INSERT. Pin max_threads
# and disable the memory-based thread clamping so that the number of parallel insert streams is
# deterministic regardless of the machine.
INSERT_SETTINGS = {
    "max_insert_threads": THREADS,
    "max_threads": 8,
    "parallel_view_processing": 1,
    "insert_deduplicate": 1,
    "deduplicate_blocks_in_dependent_materialized_views": 1,
    "max_threads_min_free_memory_per_thread": 0,
    "max_insert_threads_min_free_memory_per_thread": 0,
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def setup_tables(node):
    node.query("DROP TABLE IF EXISTS mv SYNC")
    node.query("DROP TABLE IF EXISTS dst SYNC")
    node.query("DROP TABLE IF EXISTS src SYNC")
    # `src` has no deduplication window, so it keeps every inserted block; only the
    # materialized-view target `dst` performs (view-level) deduplication. This isolates the
    # dependent-MV deduplication path from any source-table deduplication.
    node.query("CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node.query(
        "CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS non_replicated_deduplication_window = 100000"
    )
    node.query("CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src")


@pytest.mark.parametrize(
    "node_name, expected_sinks",
    [
        # Legacy hash modes must keep the dependent-MV deduplication path single-stream:
        # one MergeTreeSink for `src` plus one for the MV target `dst`.
        ("node_old", 2),
        ("node_compatible", 2),
        # new_unified_hash is safe to fan out to `max_insert_threads` streams: a MergeTreeSink
        # for `src` and one for `dst` in each of the parallel branches.
        ("node_new", 2 * THREADS),
    ],
)
def test_plain_insert_mv_dedup(start_cluster, node_name, expected_sinks):
    node = NODES[node_name]
    setup_tables(node)

    # Topology: EXPLAIN PIPELINE shows how many parallel insert streams (sinks) the writing side uses.
    pipeline = node.query(
        "EXPLAIN PIPELINE INSERT INTO src VALUES (1)",
        settings=INSERT_SETTINGS,
    )
    assert pipeline.count("MergeTreeSink") == expected_sinks, pipeline

    # Correctness: a single plain INSERT of many identical blocks must not lose rows. All blocks share
    # the same data hash, so a per-branch view block number restarting from zero (the regression this
    # guards against) would collide across parallel streams and view-level deduplication would silently
    # drop rows on the MV target `dst`.
    node.query(
        "INSERT INTO src FORMAT TSV",
        stdin="1\n" * ROWS,
        settings={
            **INSERT_SETTINGS,
            "max_block_size": 1000,
            "min_insert_block_size_rows": 1000,
        },
    )
    assert int(node.query("SELECT count() FROM src")) == ROWS
    assert int(node.query("SELECT count() FROM dst")) == ROWS
