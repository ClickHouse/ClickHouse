#!/usr/bin/env bash
# Tags: no-s3-storage, long
# Tag no-s3-storage -- merge_tree_clear_old_temporary_directories_interval_seconds
# is not supported for s3 storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that TTLDrop merges do not open source parts or read any data.
# This is important because TTLDrop merges know that all rows in all source
# parts are expired, so the merge should produce an empty part without
# allocating read buffers for source parts.
#
# We avoid OPTIMIZE TABLE FINAL because it always assigns MergeType::Regular,
# which would bypass our TTLDrop short-circuit. Instead we rely on background
# TTL merges (merge_with_ttl_timeout = 0) and wait for them to complete.

# Helper: wait until the background TTL merge completes (at most 1 active part),
# then flush logs and wait for the MergeParts entry to appear.
function wait_for_ttl_merge_and_flush_logs()
{
    local table=$1
    for _ in $(seq 1 100); do
        local part_count
        part_count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active")
        if [ "$part_count" -le "1" ]; then
            break
        fi
        sleep 0.1
    done

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"

    for _ in $(seq 1 60); do
        local count
        count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'")
        if [ "$count" -gt "0" ]; then
            return
        fi
        sleep 0.1
    done
}

# -------------------------------------------------------------------
# Case 1: Basic TTLDrop — no data should be read
# -------------------------------------------------------------------
echo "-- Case 1: Basic TTLDrop"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_no_read
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_no_read;

    INSERT INTO t_ttl_drop_no_read (id, value) SELECT number, randomString(100) FROM numbers(100);
    INSERT INTO t_ttl_drop_no_read (id, value) SELECT number, randomString(100) FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_no_read;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_no_read"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows,
        peak_memory_usage < 50000000
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_no_read'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_no_read;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_no_read;"

# -------------------------------------------------------------------
# Case 2: TTLDrop with projections — projections should be empty
# remove_empty_parts = 0 keeps the empty part produced by the TTLDrop
# merge active; otherwise the cleanup thread may drop it before the
# projections check below sees it in system.parts.
# -------------------------------------------------------------------
echo "-- Case 2: TTLDrop with projections"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_proj
    (
        id UInt64,
        value UInt64,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        PROJECTION proj_sum (SELECT id, sum(value) GROUP BY id)
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1,
        remove_empty_parts = 0;

    SYSTEM STOP MERGES t_ttl_drop_proj;

    INSERT INTO t_ttl_drop_proj (id, value) SELECT number, number FROM numbers(100);
    INSERT INTO t_ttl_drop_proj (id, value) SELECT number, number FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_proj;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_proj"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_proj'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_proj;"

# The resulting part should have no projection data.
${CLICKHOUSE_CLIENT} -q "
    SELECT projections
    FROM system.parts
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_proj'
        AND active;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_proj;"

# -------------------------------------------------------------------
# Case 3: TTLDrop with skip indexes (set, bloom_filter)
# -------------------------------------------------------------------
echo "-- Case 3: TTLDrop with skip indexes"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_idx
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        INDEX idx_set id TYPE set(100) GRANULARITY 1,
        INDEX idx_bf value TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_idx;

    INSERT INTO t_ttl_drop_idx (id, value) SELECT number, randomString(100) FROM numbers(100);
    INSERT INTO t_ttl_drop_idx (id, value) SELECT number, randomString(100) FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_idx;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_idx"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_idx'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_idx;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_idx;"

# -------------------------------------------------------------------
# Case 4: WHERE-clause TTL does NOT short-circuit
# TTLPartDropMergeSelector assigns TTLDrop based on part_max_ttl even with
# a WHERE clause. Our short-circuit detects the WHERE clause and falls
# through to the normal pipeline, so data IS read.
# -------------------------------------------------------------------
echo "-- Case 4: WHERE-clause TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_where_no_shortcircuit
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY DELETE WHERE id >= 0
    SETTINGS
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_where_no_shortcircuit;

    INSERT INTO t_ttl_where_no_shortcircuit (id, value) SELECT number, randomString(10) FROM numbers(100);
    INSERT INTO t_ttl_where_no_shortcircuit (id, value) SELECT number, randomString(10) FROM numbers(100);

    SYSTEM START MERGES t_ttl_where_no_shortcircuit;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_where_no_shortcircuit"

# Data IS read because the short-circuit detected the WHERE clause and
# fell through to the normal pipeline.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_where_no_shortcircuit'
        AND event_type = 'MergeParts'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_where_no_shortcircuit;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_where_no_shortcircuit;"

# -------------------------------------------------------------------
# Case 5: Table is fully functional after TTLDrop (insert + query + merge)
# -------------------------------------------------------------------
echo "-- Case 5: Table works after TTLDrop"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_then_insert
    (
        id UInt64,
        value String,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_then_insert;

    -- Insert expired data.
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() - INTERVAL 2 DAY FROM numbers(100);
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() - INTERVAL 2 DAY FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_then_insert;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_then_insert"

# After TTLDrop, insert fresh (non-expired) data.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() FROM numbers(50);
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_then_insert;"

# Merge the fresh data — should work as a normal merge.
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_ttl_drop_then_insert FINAL SETTINGS mutations_sync=1;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_then_insert;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_then_insert;"

# -------------------------------------------------------------------
# Case 6: Rows TTL + column TTL — not short-circuited
# hasOnlyRowsTTL is false when column TTL is present, so data IS read.
# -------------------------------------------------------------------
echo "-- Case 6: Rows TTL + column TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_col
    (
        id UInt64,
        value String TTL event_time + INTERVAL 1 HOUR,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_col;

    INSERT INTO t_ttl_col (id, value) SELECT number, randomString(10) FROM numbers(100);
    INSERT INTO t_ttl_col (id, value) SELECT number, randomString(10) FROM numbers(100);

    SYSTEM START MERGES t_ttl_col;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_col"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_col'
        AND event_type = 'MergeParts'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_col;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_col;"

# -------------------------------------------------------------------
# Case 7: Rows TTL + GROUP BY TTL — not short-circuited
# hasOnlyRowsTTL is false when GROUP BY TTL is present, so data IS read.
# After the first merge, surviving rows still have expired TTL, so
# TTLPartDropMergeSelector may re-select the single merged part for
# another TTLDrop merge. Filter by length(merged_from) > 1 to only
# look at the merge that actually combined two source parts.
# -------------------------------------------------------------------
echo "-- Case 7: Rows TTL + GROUP BY TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_groupby
    (
        id UInt64,
        value UInt64,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY GROUP BY id SET value = max(value)
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_groupby;

    INSERT INTO t_ttl_groupby (id, value) SELECT number, number FROM numbers(100);
    INSERT INTO t_ttl_groupby (id, value) SELECT number, number FROM numbers(100);

    SYSTEM START MERGES t_ttl_groupby;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_groupby"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_groupby'
        AND event_type = 'MergeParts'
        AND length(merged_from) > 1
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_groupby;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_groupby;"

# -------------------------------------------------------------------
# Case 8: a Regular merge over fully expired parts reads the source parts
#
# When TTL merge admission is unavailable, selection falls through to a
# Regular merge, and OPTIMIZE TABLE FINAL always assigns MergeType::Regular.
# Such a merge still drops every expired row, but only by re-evaluating the
# current TTL expression in the pipeline: the parts' own ttl_infos are not
# invalidated by ALTER MODIFY TTL with materialize_ttl_after_modify = 0
# (Case 11), so they cannot prove expiry under the current expression and
# only the assigned TTLDrop type may skip the read pipeline.
# max_number_of_merges_with_ttl_in_pool = 0 is the deterministic form of the
# saturated-limit state, and it keeps a background TTLDrop merge from racing.
# Merges are never stopped, so a background Regular merge racing the OPTIMIZE
# produces the same (rows, read_rows) pair; length(merged_from) > 1 keeps
# single-part FINAL merges out of the comparison.
# -------------------------------------------------------------------
echo "-- Case 8: Regular merge over fully expired parts"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_regular_fallback
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    INSERT INTO t_ttl_regular_fallback (id, value) SELECT number, randomString(100) FROM numbers(100);
    INSERT INTO t_ttl_regular_fallback (id, value) SELECT number, randomString(100) FROM numbers(100);

    OPTIMIZE TABLE t_ttl_regular_fallback FINAL;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_regular_fallback"

${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_regular_fallback'
        AND event_type = 'MergeParts'
        AND length(merged_from) > 1
    ORDER BY merge_reason, rows, read_rows;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_regular_fallback;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_regular_fallback;"

# -------------------------------------------------------------------
# Case 9: a pending patch part disables the short-circuit
#
# The proof of expiry comes from each source part's own ttl_infos, which were
# written before any lightweight update and so cannot describe the patched rows.
# Patches are applied by the merge itself (apply_patches_on_merge, on by
# default), so a merge carrying one has real work to do and must not skip the
# read pipeline.
# -------------------------------------------------------------------
echo "-- Case 9: pending patch part disables the short-circuit"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patched
    (
        id UInt64,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patched;

    -- Every row is expired when the part is written, so its ttl_infos say 'fully expired'.
    INSERT INTO t_ttl_patched SELECT number, now() - INTERVAL 2 DAY FROM numbers(100);

    -- A lightweight update leaves a patch part that un-expires 10 of them.
    UPDATE t_ttl_patched SET event_time = now() + INTERVAL 2 DAY WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM START MERGES t_ttl_patched;

    OPTIMIZE TABLE t_ttl_patched FINAL;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"

# The merge must open the source parts, because the patch has to be applied before TTL can be
# evaluated. The pre-patch infos must not feed the drop-all fast path either: the 10 patched rows
# are no longer expired and must survive the merge, which also separates it from the shortcut.
${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT
        merge_reason,
        rows,
        read_rows >= 100
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_patched'
        AND event_type = 'MergeParts'
        AND length(merged_from) > 0
    ORDER BY ALL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patched;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patched;"

# -------------------------------------------------------------------
# Case 10: SYSTEM STOP TTL MERGES still suppresses row removal
#
# OPTIMIZE ... FINAL assigns MergeType::Regular, so the merge goes through
# the pipeline; the same flag that gates the shortcut also gates the
# pipeline's TTL step (MergeTask.cpp clears need_remove_expired_values when
# TTL work is cancelled), so the rows survive either way. The operator has
# stopped TTL merges, so no data may be dropped.
# -------------------------------------------------------------------
echo "-- Case 10: STOP TTL MERGES suppresses the short-circuit"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_stopped
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_stopped;
    SYSTEM STOP TTL MERGES t_ttl_stopped;

    INSERT INTO t_ttl_stopped (id, value) SELECT number, randomString(100) FROM numbers(100);
    INSERT INTO t_ttl_stopped (id, value) SELECT number, randomString(100) FROM numbers(100);

    -- Only ordinary merges are re-enabled; TTL merges stay stopped.
    SYSTEM START MERGES t_ttl_stopped;

    OPTIMIZE TABLE t_ttl_stopped FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_stopped;"
${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_stopped;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_stopped;"

# -------------------------------------------------------------------
# Case 11: MODIFY TTL without materialization still reads the source parts
#
# materialize_ttl_after_modify = 0 deliberately leaves old parts in place,
# so their ttl_infos stay computed under the previous TTL expression: a part
# fully expired under the original short TTL still says so after the TTL is
# relaxed. A Regular merge over such a part must not skip the read on the
# strength of that stale metadata -- this case pins read_rows so the
# zero-read shortcut can never fire here. The rows themselves are dropped
# by the ordinary TTL filter, which legitimately trusts the same stale
# ttl_infos while the user opted out of materialization; recomputing them
# under the relaxed expression is upstream behavior, unchanged by this PR.
# max_number_of_merges_with_ttl_in_pool = 0 makes OPTIMIZE TABLE FINAL the
# deterministic Regular merge here, and keeps the stale parts away from the
# TTL drop selector. Merges are stopped until the ALTER has relaxed the TTL,
# so no merge can run under the short one; afterwards a racing background
# Regular merge produces the same (rows, read_rows) pair, so the part_log
# comparison below is order-independent.
# -------------------------------------------------------------------
echo "-- Case 11: MODIFY TTL without materialization still reads the source parts"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_modify_relaxed
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_modify_relaxed;

    -- Both parts are fully expired under the original TTL, and their ttl_infos say so.
    INSERT INTO t_ttl_modify_relaxed (id, value) SELECT number, randomString(100) FROM numbers(100);
    INSERT INTO t_ttl_modify_relaxed (id, value) SELECT number, randomString(100) FROM numbers(100);

    -- Relax the TTL without materializing it: the parts keep the stale, expired infos.
    SET materialize_ttl_after_modify = 0;
    ALTER TABLE t_ttl_modify_relaxed MODIFY TTL event_time + INTERVAL 10 YEAR;

    SYSTEM START MERGES t_ttl_modify_relaxed;

    OPTIMIZE TABLE t_ttl_modify_relaxed FINAL;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_modify_relaxed"

# The merge must open the source parts: a zero-read shortcut trusting the
# stale ttl_infos would report (0, 0) here. The ordinary TTL filter then drops
# the rows per the same stale infos, which the materialization opt-out allows.
${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_modify_relaxed'
        AND event_type = 'MergeParts'
        AND length(merged_from) > 1
    ORDER BY merge_reason, rows, read_rows;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_modify_relaxed;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_modify_relaxed;"

# -------------------------------------------------------------------
# Case 12: a patch that newly expires rows forces the TTL step
#
# The source part's own ttl_infos say nothing is due (every event_time is in
# the future), so pre-patch metadata alone would skip TTL work entirely. The
# patch moves 10 rows into the past; the merge must re-evaluate TTL against
# the patched values and drop exactly those rows.
# -------------------------------------------------------------------
echo "-- Case 12: patch that newly expires rows forces the TTL step"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_expire
    (
        id UInt64,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_expire;

    -- Nothing is expired when the part is written.
    INSERT INTO t_ttl_patch_expire SELECT number, now() + INTERVAL 2 DAY FROM numbers(100);

    -- A lightweight update expires 10 rows; only the patch part knows.
    UPDATE t_ttl_patch_expire SET event_time = now() - INTERVAL 2 DAY WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM START MERGES t_ttl_patch_expire;

    OPTIMIZE TABLE t_ttl_patch_expire FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_expire;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_expire;"

# -------------------------------------------------------------------
# Case 13: a blocked TTL merge still recalculates patched infos
#
# TTL removal is stopped, so all 100 rows survive the merge; but the merged
# part must carry post-patch ttl_infos - with the stale pre-patch metadata a
# later TTLDrop merge could treat the part as fully expired and drop the 10
# rows the patch moved back into the future.
# -------------------------------------------------------------------
echo "-- Case 13: blocked TTL merge still recalculates patched infos"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_blocked
    (
        id UInt64,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1;

    SYSTEM STOP MERGES t_ttl_patch_blocked;

    -- Every row is expired when the part is written.
    INSERT INTO t_ttl_patch_blocked SELECT number, now() - INTERVAL 2 DAY FROM numbers(100);

    -- A lightweight update un-expires 10 of them.
    UPDATE t_ttl_patch_blocked SET event_time = now() + INTERVAL 2 DAY WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_patch_blocked;
    SYSTEM START MERGES t_ttl_patch_blocked;

    OPTIMIZE TABLE t_ttl_patch_blocked FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_blocked;"

# The recalculated maximum covers the patched future rows, so the part cannot read as fully expired.
${CLICKHOUSE_CLIENT} -q "
    SELECT max(delete_ttl_info_max) > now() + INTERVAL 1 DAY
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_blocked' AND active AND rows = 100;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_blocked;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_blocked;"

# -------------------------------------------------------------------
# Case 14: vertical TTL merge keeps move/recompress inputs readable
#
# The TTL step of a vertical merge (canVerticalTTLDelete) runs on the
# horizontal stream, which used to carry only the rows-TTL inputs; the
# MOVE/RECOMPRESS rebuild then failed with NOT_FOUND_COLUMN_IN_BLOCK.
# Patch-forced TTL steps ride the same stream, so they need this too.
# -------------------------------------------------------------------
echo "-- Case 14: vertical TTL merge keeps move/recompress inputs readable"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_vertical_recompress
    (
        id UInt64,
        event_time DateTime,
        d2 DateTime,
        pad String
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 SECOND, d2 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3))
    SETTINGS
        -- Every input of chooseMergeAlgorithm is pinned here: the runner randomizes these same
        -- MergeTree settings per run, and any one of them can send the merge down the horizontal
        -- path, where this case would no longer exercise anything.
        enable_vertical_merge_algorithm = 1,
        vertical_merge_optimize_ttl_delete = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0,
        allow_vertical_merges_from_compact_to_wide_parts = 1,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_ttl_vertical_recompress;

    -- Half the rows are already expired, half survive, in both parts.
    INSERT INTO t_ttl_vertical_recompress
        SELECT number, if(number % 2 = 0, now() - INTERVAL 1 HOUR, now() + INTERVAL 10 HOUR), now(), repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_ttl_vertical_recompress
        SELECT number + 1000, if(number % 2 = 0, now() - INTERVAL 1 HOUR, now() + INTERVAL 10 HOUR), now(), repeat('y', 100) FROM numbers(1000);

    SYSTEM START MERGES t_ttl_vertical_recompress;

    OPTIMIZE TABLE t_ttl_vertical_recompress FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_vertical_recompress;"

# NOTE: this is a smoke check, not a regression for the vertical-merge fix. The horizontal path
# always carried the MOVE/RECOMPRESS input columns, so it satisfies the assertion below without
# exercising the change, and which algorithm chooseMergeAlgorithm picks here is not stable across
# CI build flavors (pinning every one of its documented inputs at table level did not make it so).
# The vertical path reproduces deterministically on a local server, where an unfixed build aborts
# this merge with NOT_FOUND_COLUMN_IN_BLOCK. The regression for it is
# tests/integration/test_ttl_vertical_merge_recompress, where the server config is fixed and the
# merge algorithm is asserted, so it cannot pass on the horizontal path.
${CLICKHOUSE_CLIENT} -q "
    SELECT any(recompression_ttl_info.max[1]) > now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vertical_recompress' AND active AND rows = 1000;
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_vertical_recompress;"

# -------------------------------------------------------------------
# Case 15: patched merge does not recompress on pre-patch infos
#
# The output codec used to be chosen from the aggregated pre-patch
# recompression infos; a patch moving the TTL input into the future then
# recompressed prematurely. The codec must come from the table default and
# the recalculated infos must reflect the patched values.
# -------------------------------------------------------------------
echo "-- Case 15: patched merge does not recompress on pre-patch infos"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_recompress
    (
        id UInt64,
        d2 DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL d2 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3))
    SETTINGS
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_recompress;

    -- The recompression TTL is satisfied when the part is written...
    INSERT INTO t_ttl_patch_recompress SELECT number, now() - INTERVAL 2 DAY FROM numbers(100);

    -- ...but a lightweight update moves 10 inputs back into the future.
    UPDATE t_ttl_patch_recompress SET d2 = now() WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM START MERGES t_ttl_patch_recompress;

    OPTIMIZE TABLE t_ttl_patch_recompress FINAL;
"

# The recalculated recompression info reflects the patched values, so a later
# recompression merge applies the codec once the TTL is really due.
${CLICKHOUSE_CLIENT} -q "
    SELECT any(recompression_ttl_info.max[1]) > now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_recompress' AND active AND rows = 100;
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_recompress;"

# -------------------------------------------------------------------
# Case 16: a patched merge cannot keep a finished rows-WHERE TTL entry
#
# TTLDeleteAlgorithm marks the recalculated entry finished off the old
# (pre-patch) max, hiding the merged part from later TTL passes; rows a
# patch moved into the near future then overstayed forever.
# -------------------------------------------------------------------
echo "-- Case 16: patched merge keeps rows-WHERE TTL entries live"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_rows_where
    (
        id UInt64,
        event_time DateTime,
        flag UInt8
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time DELETE WHERE flag = 1
    SETTINGS
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_rows_where;

    -- Every row is expired when the part is written.
    INSERT INTO t_ttl_patch_rows_where SELECT number, now() - INTERVAL 2 DAY, 1 FROM numbers(100);

    -- A lightweight update moves 10 rows into the near future.
    UPDATE t_ttl_patch_rows_where SET event_time = now() + INTERVAL 10 SECOND WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM START MERGES t_ttl_patch_rows_where;

    OPTIMIZE TABLE t_ttl_patch_rows_where FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_rows_where;"

# Once their TTL is really due, a merge must still see the part as TTL work: a finished entry
# would exclude it from the part's TTL bounds and the rows would survive every later OPTIMIZE.
# The patched rows fall due 10 seconds after the UPDATE above, so wait past that deadline once
# rather than polling through it - the retries below only absorb a slow merge, not the wait.
sleep 11
rows_where_left=""
for _ in $(seq 1 15); do
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_ttl_patch_rows_where FINAL;"
    rows_where_left=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_rows_where;")
    [ "$rows_where_left" = "0" ] && break
    sleep 1
done
echo "$rows_where_left"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_rows_where;"


# Case 17: the recalculation step honours a rows-WHERE predicate
#
# TTLUpdateInfoAlgorithm folds every row into the recorded min/max and never
# evaluates `where_expression`. With TTL merges stopped the recalculation step
# is the only thing that repopulates that family, so an unfiltered fold records
# a bound belonging to a row the rule can never delete, and the part looks due
# for a TTL merge that would find nothing to drop.
# -------------------------------------------------------------------
echo "-- Case 17: the recalculation step honours a rows-WHERE predicate"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_rows_where_predicate (id UInt64, ts DateTime, flag UInt8)
    ENGINE = MergeTree()
    ORDER BY id
    TTL ts + INTERVAL 50 YEAR DELETE WHERE flag = 1
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    -- The oldest ts belongs to a flag = 0 row, which this rule can never delete, so it must not
    -- become the bound. Inserted before TTL merges are stopped so no background TTL pass can rewrite
    -- the part first; STOP TTL MERGES stops those merges, it does not block inserts.
    INSERT INTO t_ttl_rows_where_predicate VALUES (1, '2000-01-01 00:00:00', 0), (3, '2030-01-01 00:00:00', 1);
    INSERT INTO t_ttl_rows_where_predicate VALUES (2, '2020-01-01 00:00:00', 1);

    SYSTEM STOP TTL MERGES t_ttl_rows_where_predicate;
    OPTIMIZE TABLE t_ttl_rows_where_predicate FINAL;
"

# 2070 is the oldest flag = 1 row's TTL; folding the flag = 0 row in would give 2050.
${CLICKHOUSE_CLIENT} -q "
    SELECT rows_where_ttl_info.min[1] = toDateTime('2020-01-01 00:00:00') + INTERVAL 50 YEAR
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_rows_where_predicate' AND active;
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_rows_where_predicate;"

# -------------------------------------------------------------------
# Case 18: a blocked patched merge pulls part_min_ttl back into the past
#
# Case 13 pins the max side, where the patch moves rows into the future. The
# mirror is what the selectors actually schedule from: a part whose rows are
# all in the future is not TTL-due, and a patch that expires some of them must
# pull part_min_ttl into the past. Copied pre-patch infos leave it in the
# future, so TTLMergeSelector never picks the part up and the expired rows
# overstay. part_min_ttl is not a column, but with a table-level DELETE TTL as
# the only TTL it is exactly delete_ttl_info_min.
# -------------------------------------------------------------------
echo "-- Case 18: blocked patched merge pulls part_min_ttl into the past"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_min
    (
        id UInt64,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 0,
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_min;

    -- Every row is far in the future, so the part is not TTL-due at all.
    INSERT INTO t_ttl_patch_min SELECT number, now() + INTERVAL 10 DAY FROM numbers(100);

    -- A lightweight update expires 10 of them.
    UPDATE t_ttl_patch_min SET event_time = now() - INTERVAL 10 DAY WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_patch_min;
    SYSTEM START MERGES t_ttl_patch_min;
    OPTIMIZE TABLE t_ttl_patch_min FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_min;"

# Stale pre-patch infos leave both bounds in the future; the recalculated pair straddles now().
${CLICKHOUSE_CLIENT} -q "
    SELECT max(delete_ttl_info_min) < now() AND max(delete_ttl_info_max) > now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_min' AND active AND rows = 100;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_min;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_min;"

# -------------------------------------------------------------------
# Case 19: a blocked patched merge keeps its GROUP BY TTL entry
#
# The recalculation step Case 18 relies on runs for GROUP BY tables too, but it
# rebuilds that family through TTLUpdateInfoAlgorithm, which never sets
# ttl_finished - an already-expired entry would come back unfinished and the
# part would be reselected for TTL forever, the shape 04501 pins. The pre-patch
# entry is snapshotted and put back instead, so a patch moving rows into the
# future must not widen the GROUP BY bound.
# -------------------------------------------------------------------
echo "-- Case 19: blocked patched merge keeps its GROUP BY TTL entry"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_group_by
    (
        id UInt64,
        event_time DateTime,
        value UInt64
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY GROUP BY id SET value = max(value)
    SETTINGS
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_group_by;

    -- Every row is already past its GROUP BY deadline when the part is written.
    INSERT INTO t_ttl_patch_group_by SELECT number, now() - INTERVAL 2 DAY, number FROM numbers(100);

    -- A lightweight update moves 10 of them well into the future.
    UPDATE t_ttl_patch_group_by SET event_time = now() + INTERVAL 5 DAY WHERE id < 10
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_patch_group_by;
    SYSTEM START MERGES t_ttl_patch_group_by;
    OPTIMIZE TABLE t_ttl_patch_group_by FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by;"

# Rebuilt from the patched values the bound would sit 6 days out; carried across it stays expired.
${CLICKHOUSE_CLIENT} -q "
    SELECT max(group_by_ttl_info.max[1]) < now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_group_by' AND active AND rows = 100;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_group_by;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by;"
