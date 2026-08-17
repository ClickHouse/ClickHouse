#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the async insert queue to coalesce several tokens into one flush.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111031
# When several async-insert entries with distinct insert_deduplication_token values are
# coalesced into one flush, each token used to be registered in the dedup log of EVERY
# partition the flush touched, not only the partition its own rows landed in. A later
# legitimately-distinct insert reusing one of those tokens in a partition it never wrote
# to was then silently deduplicated away (silent data loss). Affects MergeTree and
# ReplicatedMergeTree (shared sink split logic).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Common async settings: long busy timeout so nothing auto-fires; one explicit flush
# coalesces the queued entries into a single batch. insert_deduplication_token is excluded
# from the async queue key, so entries with distinct tokens land in ONE flush.
async_insert=(--async_insert=1 --wait_for_async_insert=0
    --async_insert_busy_timeout_min_ms=600000 --async_insert_busy_timeout_max_ms=600000
    --async_insert_use_adaptive_busy_timeout=0 --insert_deduplicate=1)

run_engine() {
    local label=$1 engine=$2 table=$3

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $table (p UInt8, x UInt64)
    ENGINE = $engine PARTITION BY p ORDER BY x
    SETTINGS non_replicated_deduplication_window = 1000, deduplicate_merge_projection_mode = 'drop'"

    # Coalesce two entries: token A lands only in p=0, token B lands only in p=1.
    $CLICKHOUSE_CLIENT "${async_insert[@]}" --insert_deduplication_token='A' -q "INSERT INTO $table VALUES (0, 10)"
    $CLICKHOUSE_CLIENT "${async_insert[@]}" --insert_deduplication_token='B' -q "INSERT INTO $table VALUES (1, 20)"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE $table"

    # Token A was never used in p=1, so this row must be accepted (was dropped before the fix).
    $CLICKHOUSE_CLIENT "${async_insert[@]}" --insert_deduplication_token='A' -q "INSERT INTO $table VALUES (1, 30)"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE $table"

    # Expected: [(0,10),(1,20),(1,30)]. Before the fix (1,30) was silently dropped -> count 2.
    # enable_parallel_replicas=0: this check verifies inserted data, not the read path; parallel
    # replicas need a matching cluster topology that this test does not set up.
    $CLICKHOUSE_CLIENT -q "SELECT '$label', groupArray((p, x)), count() FROM (SELECT p, x FROM $table ORDER BY p, x) SETTINGS enable_parallel_replicas = 0"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $table"
}

run_engine 'MergeTree' 'MergeTree' 'bleed_mt'
run_engine 'ReplicatedMergeTree' "ReplicatedMergeTree('/clickhouse/tables/{database}/bleed_rmt', 'r1')" 'bleed_rmt'

# A single async entry whose one token legitimately spans two partitions must still
# deduplicate on a full re-insert (the token belongs in both partitions it wrote to).
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS single_multi"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE single_multi (p UInt8, x UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS non_replicated_deduplication_window = 1000"
$CLICKHOUSE_CLIENT "${async_insert[@]}" --insert_deduplication_token='T' -q "INSERT INTO single_multi VALUES (0, 1), (1, 2)"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE single_multi"
$CLICKHOUSE_CLIENT "${async_insert[@]}" --insert_deduplication_token='T' -q "INSERT INTO single_multi VALUES (0, 1), (1, 2)"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE single_multi"
# Expected: [(0,1),(1,2)] count 2 (fully deduplicated, not doubled).
$CLICKHOUSE_CLIENT -q "SELECT 'single-entry-multi-partition', groupArray((p, x)), count() FROM (SELECT p, x FROM single_multi ORDER BY p, x) SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE single_multi"
