#!/usr/bin/env bash
# Regression test for plain INSERTs into storages that forward the write through a nested INSERT
# (Alias, Distributed) with max_insert_threads and active deduplication. A DistributedSink opens a
# remote or local insert per branch that stamps the deduplication info from scratch, so the source
# block numbering restarts at zero on every branch: two identical blocks on different branches get
# identical deduplication ids and a deduplicating target MergeTree silently drops one of them - the
# fan-out must fail closed for Distributed. An AliasSink runs its nested INSERT in this query's
# context with the chunk's deduplication info intact, and an already-stamped chunk is not restamped,
# so without strict insert block limits the globally stamped numbering survives the hop, the ids stay
# distinct across branches, and the INSERT fans out like one into the alias target itself.
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

# An Alias of a deduplicating table with deduplication active: the INSERT fans out, because without
# strict insert block limits the source block numbering is stamped globally in the single-stream head
# of the pipeline and survives each branch's AliasSink hop (the nested INSERT does not restamp an
# already-stamped chunk), so identical blocks on different branches keep distinct ids - the same
# reason a plain INSERT into the deduplicating target itself fans out.
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

# Deduplication disabled for the outer query does NOT let the Distributed table fan out either: the
# remote shard table is not known here and may be (or forward to) a Buffer, whose flush runs in its
# own context and can re-enable deduplication while each parallel branch restarts the source block
# numbering from zero. So the fan-out fails closed regardless of the outer deduplicate_insert.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Row integrity through the alias with the fan-out active: four identical 100-row blocks must all
# arrive. If the nested INSERT restamped the numbering per branch, identical blocks on different
# branches would collide and the deduplicating target would silently drop rows. Kept intentionally
# small so the test stays well under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO fwd_dedup_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM fwd_dedup_target"

# A forwarding storage whose immediate target never deduplicates but which has a deduplicating
# dependent materialized view (fwd_dedup_mv_src -> fwd_dedup_mv_mv -> fwd_dedup_mv_dst): the
# src -> mv -> dst chain lives behind the AliasSink's nested INSERT and is not visible to
# InsertDependenciesBuilder, so the safety of the fan-out is decided by the outer guard. Without
# strict insert block limits the source numbering is global and survives the hop, the view-level ids
# fold it in and stay distinct across branches, so the INSERT fans out; under
# use_strict_insert_block_limits the numbering is per-branch and the ids of identical blocks would
# collide on the deduplicating MV target, so the fan-out fails closed (see 04611).
MV_SETTINGS="--parallel_view_processing=1 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW fwd_dedup_mv_mv TO fwd_dedup_mv_dst AS SELECT x FROM fwd_dedup_mv_src"
$CLICKHOUSE_CLIENT -q "CREATE TABLE fwd_dedup_mv_alias ENGINE = Alias('fwd_dedup_mv_src')"

# Dependent-MV deduplication active, no strict limits: the INSERT fans out - the global source
# numbering survives the alias hop and keeps the view-level ids distinct across branches.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_mv_alias VALUES (1)" | grep -c "AliasSink"

# Deduplication disabled for the session: no target consults the ids, so the INSERT fans out.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO fwd_dedup_mv_alias VALUES (1)" | grep -c "AliasSink"

# Row integrity through the alias into the deduplicating MV target with the fan-out active: four
# identical 100-row blocks must all arrive in the MV target.
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
