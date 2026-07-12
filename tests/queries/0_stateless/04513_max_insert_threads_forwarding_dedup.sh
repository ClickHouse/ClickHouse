#!/usr/bin/env bash
# Regression test: plain INSERTs into storages that forward the write through a nested INSERT
# (Alias, Distributed) must not fan out to max_insert_threads sink branches while deduplication is
# active. Each branch's sink executes its own nested INSERT (AliasSink runs a full insert pipeline,
# DistributedSink opens a remote or local insert per branch), which stamps the deduplication info
# from scratch, so the source block numbering restarts at zero on every branch. Two identical blocks
# on different branches then get identical deduplication ids and a deduplicating target MergeTree
# silently drops one of them. The fan-out must fail closed for such storages; it keeps applying when
# deduplication is disabled for the session or the target provably never deduplicates.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS fwd_dedup_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS fwd_dedup_alias_nodedup"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS fwd_dedup_dist"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS fwd_dedup_target"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS fwd_dedup_target_nodedup"

$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_target (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_target_nodedup (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_alias ENGINE = Alias('fwd_dedup_target')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_alias_nodedup ENGINE = Alias('fwd_dedup_target_nodedup')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), fwd_dedup_target)"

# An Alias of a deduplicating table with deduplication active: a single sink, because every branch
# would run its own nested INSERT with a per-branch block numbering.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_alias VALUES (1)" | grep -c "AliasSink"

# Deduplication disabled for the session (deduplicate_insert = disable): no sink consults the ids,
# so the INSERT fans out.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_alias VALUES (1)" | grep -c "AliasSink"

# An Alias of a table that never deduplicates: the target is known locally, so the INSERT fans out.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_alias_nodedup VALUES (1)" | grep -c "AliasSink"

# A Distributed table: its ultimate target is not cheaply known here (it may be a deduplicating
# MergeTree, as in this test), so the fan-out fails closed while deduplication is active.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Deduplication disabled for the session: the INSERT into the Distributed table fans out.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Row integrity through the alias: four identical 100-row blocks must all arrive. A per-branch
# nested INSERT would restart the numbering at zero, collide the ids of identical blocks across
# branches, and the deduplicating target would silently drop rows. Kept intentionally small so the
# test stays well under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO fwd_dedup_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM fwd_dedup_target"

# A forwarding storage whose immediate target never deduplicates but which has a deduplicating
# dependent materialized view (fwd_dedup_mv_src -> fwd_dedup_mv_mv -> fwd_dedup_mv_dst) must also stay
# single-stream while dependent-MV deduplication is active. The outer INSERT into the alias only sees
# the AliasSink; the src -> mv -> dst chain lives behind the AliasSink's nested INSERT and is not
# visible to InsertDependenciesBuilder, so the fallback is decided by the outer guard. A per-branch
# nested INSERT would restart the source block numbering, collide the view-level ids of identical
# blocks across branches, and the deduplicating MV target would silently drop rows.
MV_SETTINGS="--parallel_view_processing=1 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW fwd_dedup_mv_mv TO fwd_dedup_mv_dst AS SELECT x FROM fwd_dedup_mv_src"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_alias ENGINE = Alias('fwd_dedup_mv_src')"

# Dependent-MV deduplication active: a single AliasSink even though the alias target never deduplicates.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_mv_alias VALUES (1)" | grep -c "AliasSink"

# Deduplication disabled for the session: no target consults the ids, so the INSERT fans out.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_mv_alias VALUES (1)" | grep -c "AliasSink"

# Row integrity through the alias into the deduplicating MV target: four identical 100-row blocks
# must all arrive in the MV target.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO fwd_dedup_mv_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM fwd_dedup_mv_dst"

$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_mv_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_mv_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_mv_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_mv_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_dist"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_alias_nodedup"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_target_nodedup"
$CLICKHOUSE_CLIENT -q "DROP TABLE fwd_dedup_target"
