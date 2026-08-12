#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a BACKGROUND cleanup merge. Whether a background merge runs with CLEANUP is decided at selection time
# (StorageMergeTree::selectPartsToMerge derives it from future_part->final and the min_age_to_force_merge_* /
# replacing-merge-with-cleanup settings) and carried to the scheduler on the selected entry, so the reservation
# prices the merge as row-reducing - it removes deleted rows and rebuilds projections - instead of as an
# ordinary merge. Under a pathologically small soft limit a single merge is still always admitted alone, so the
# background cleanup merge must run to a single part with the deleted rows removed and the projection rebuilt,
# and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04869_merge_memory_reservation_background_cleanup_XXXXXX")

${CLICKHOUSE_LOCAL} --path "$DATA_DIR" -q "
    CREATE TABLE t_merge_mem_background_cleanup
    (
        k UInt64,
        v String,
        ver UInt64,
        is_deleted UInt8,
        PROJECTION p_cleanup (SELECT k, v ORDER BY v)
    )
    ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k
    SETTINGS allow_experimental_replacing_merge_with_cleanup = 1,
             enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1,
             min_age_to_force_merge_seconds = 1,
             min_age_to_force_merge_on_partition_only = 1,
             deduplicate_merge_projection_mode = 'rebuild',
             materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_background_cleanup;
    INSERT INTO t_merge_mem_background_cleanup SELECT number, repeat('a', 100), 1, 0 FROM numbers(2000);
    INSERT INTO t_merge_mem_background_cleanup SELECT number, repeat('b', 100), 2, number % 2 FROM numbers(2000);
" -- --merges_mutations_memory_usage_soft_limit=1 < /dev/null

# The cleanup merge is scheduled by the background executor once the parts are older than
# min_age_to_force_merge_seconds; every poll below keeps a server alive for a few seconds to give the
# scheduler a window, and exits as soon as the parts have merged.
for _ in $(seq 1 60)
do
    parts=$(${CLICKHOUSE_LOCAL} --path "$DATA_DIR" -q "
        SELECT sleepEachRow(1) FROM numbers(3) FORMAT Null;
        SELECT count() FROM system.parts WHERE table = 't_merge_mem_background_cleanup' AND active;
    " -- --merges_mutations_memory_usage_soft_limit=1 < /dev/null)
    if [ "$parts" = "1" ]; then
        break
    fi
done

${CLICKHOUSE_LOCAL} --path "$DATA_DIR" -q "
    SELECT count() FROM system.parts WHERE table = 't_merge_mem_background_cleanup' AND active;
    -- The merge ran with CLEANUP: the rows marked is_deleted are physically removed, not merely collapsed.
    SELECT count() FROM t_merge_mem_background_cleanup;
    SELECT name, rows FROM system.projection_parts WHERE table = 't_merge_mem_background_cleanup' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1 < /dev/null

rm -rf "$DATA_DIR"
