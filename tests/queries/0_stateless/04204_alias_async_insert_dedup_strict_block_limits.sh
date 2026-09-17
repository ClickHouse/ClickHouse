#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: needs ZooKeeper for ReplicatedMergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# --- Part 1: async insert into Alias with strict limits must not abort the server ---
# Crash regression for the chassert 'block.rows() == getRows()' in DeduplicationInfo. Observable on
# debug/asan builds where the assert fires; on a release build it only proves the server survived.

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS alias_strict_target SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS alias_strict_alias SYNC"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE alias_strict_target (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alias_strict', '{replica}')
    ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE alias_strict_alias ENGINE = Alias(currentDatabase(), 'alias_strict_target')"

# Queue 20 async inserts from a single connection into one flush batch (a busy timeout long
# enough that nothing auto-fires, then one explicit flush). The queue batches them into a single
# multi-token DeduplicationInfo (one token per entry) whose offsets describe the whole batch.
# Routed through the alias nested pipeline with strict block limits and max block size 2, the
# unfixed code sliced that chunk and aborted the server in DeduplicationInfo. One client process
# keeps memory low under sanitizers.
QUERIES=""
for i in $(seq 1 20); do
    QUERIES="$QUERIES INSERT INTO alias_strict_alias (a) VALUES ($i),($((i+50))),($((i+100)));"
done
${CLICKHOUSE_CLIENT} \
    --async_insert=1 --wait_for_async_insert=0 \
    --async_insert_busy_timeout_max_ms=600000 --async_insert_busy_timeout_min_ms=600000 \
    --async_insert_use_adaptive_busy_timeout=0 \
    --insert_deduplicate=1 --async_insert_deduplicate=1 \
    --use_strict_insert_block_limits=1 --max_insert_block_size=2 --min_insert_block_size_rows=2 \
    --multiquery -q "$QUERIES"
# Table-scoped flush (the queue key stores the resolved INSERT table_id) so we drain only this
# test's batch instead of flushAll, which sets flush_stopped and serializes unrelated parallel tests.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE alias_strict_alias"

# If the server survived (no abort), it answers and the rows are present.
${CLICKHOUSE_CLIENT} -q "SELECT 'ok', count() > 0 FROM alias_strict_target"

${CLICKHOUSE_CLIENT} -q "DROP TABLE alias_strict_alias SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE alias_strict_target SYNC"

# --- Part 2: async insert through Alias must use ASYNC deduplication, like a direct insert ---
# User-visible on every build (no chassert needed), so it discriminates the bug under the release
# build used by Bugfix validation. The unfixed code ran the alias target pipeline with
# async_insert=false, so it picked the SYNC deduplication window; the fix threads async_insert
# through, so it picks the async window like a direct insert.
#
# Both targets disable the sync window and enable the async one. A direct async insert deduplicates
# the two identical batches via the async window (3 rows). The alias must behave identically. With
# the unfixed code the alias used the sync window (= 0, no dedup) and kept both batches (6 rows).
#
# NOTE: this discriminates the bug only while the legacy per-source async window is honored
# (insert_deduplication_version = old_separate_hashes / compatible_double_hashes). Under the default
# new_unified_hash both the direct and the alias paths share the sync window, so this parity holds
# (6 == 6) even if the alias sink drops async_insert. The version-pinned coverage lives in the
# integration test
# test_migration_deduplication_hash::test_alias_async_insert_uses_async_window_compatible.

two_identical_async_batches() {
    local table=$1
    ${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --async_insert_deduplicate=1 \
        -q "INSERT INTO $table VALUES (1),(2),(3)"
    ${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --async_insert_deduplicate=1 \
        -q "INSERT INTO $table VALUES (1),(2),(3)"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dedup_direct SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dedup_target SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dedup_alias SYNC"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE dedup_direct (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dedup_direct', '{replica}')
    ORDER BY a
    SETTINGS replicated_deduplication_window = 0, replicated_deduplication_window_for_async_inserts = 10000"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE dedup_target (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dedup_target', '{replica}')
    ORDER BY a
    SETTINGS replicated_deduplication_window = 0, replicated_deduplication_window_for_async_inserts = 10000"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dedup_alias ENGINE = Alias(currentDatabase(), 'dedup_target')"

two_identical_async_batches dedup_direct
two_identical_async_batches dedup_alias

# The alias count must equal the direct count (3, deduplicated via the async window). The unfixed
# code produced 6 for the alias, so the equality fails there.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'parity', (SELECT count() FROM dedup_alias) = (SELECT count() FROM dedup_direct)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE dedup_alias SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dedup_target SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dedup_direct SYNC"
