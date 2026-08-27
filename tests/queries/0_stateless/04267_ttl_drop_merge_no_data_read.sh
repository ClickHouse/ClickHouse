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

# The merge must open the source parts, because the patch has to be applied before the rows can
# be dropped. read_rows is what separates that from the short-circuit, which reports 0 -- the row
# count cannot, since these rows are expired either way.
${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_patched'
        AND event_type = 'MergeParts'
        AND length(merged_from) > 0
    ORDER BY merge_reason, rows, read_rows;
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
# Case 11: MODIFY TTL without materialization keeps the rows on a Regular merge
#
# materialize_ttl_after_modify = 0 deliberately leaves old parts in place,
# so their ttl_infos stay computed under the previous TTL expression: a part
# fully expired under the original short TTL still says so after the TTL is
# relaxed. A Regular merge over such a part must not trust that metadata --
# it re-evaluates the current TTL in the pipeline and keeps the rows, which
# is exactly what the zero-read shortcut would have skipped.
# max_number_of_merges_with_ttl_in_pool = 0 makes OPTIMIZE TABLE FINAL the
# deterministic Regular merge here, and keeps the stale parts away from the
# TTL drop selector, which would drop them as expired. Merges are stopped
# until the ALTER has relaxed the TTL, so no merge can run under the short
# one; afterwards a racing background Regular merge keeps the rows too, so
# the part_log comparison below is order-independent.
# -------------------------------------------------------------------
echo "-- Case 11: MODIFY TTL without materialization keeps the rows"

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

# The merge must open the source parts and keep what it reads: the rows are
# alive under the current TTL. A zero-read shortcut trusting the stale
# ttl_infos would report (0, 0) here and leave the table empty.
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
