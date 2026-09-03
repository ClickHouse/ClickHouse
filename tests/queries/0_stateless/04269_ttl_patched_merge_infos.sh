#!/usr/bin/env bash
# Tags: no-s3-storage, long
# Tag no-s3-storage -- mirrors 04267, whose patched-merge cases these are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# TTL infos on merges that carry a patch. Split out of 04267: the flaky check re-runs a changed
# test five times, and the combined file sat at ~336s against a 600s per-test wall, so any
# slowdown tipped it over.

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
    WHERE database = currentDatabase() AND table = 't_ttl_patch_recompress' AND active
      AND partition_id NOT LIKE 'patch-%';
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

# -------------------------------------------------------------------
# Case 22: an unblocked patch moving every row into the future keeps the bound
#
# Case 21's mirror on the ordinary path: there the TTL blocker is on, here the TTL
# step runs. TTLAggregationAlgorithm marks its entry finished off the pre-patch
# `max`, and with every row patched forward it aggregates nothing, so `finalize`
# writes that untouched entry out and the merged part is left with a zero bound.
# -------------------------------------------------------------------
echo "-- Case 22: an unblocked patch moving every row into the future keeps the bound"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_group_by_all_future
    (
        id UInt64,
        event_time DateTime,
        value UInt64
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY GROUP BY id SET value = max(value)
    SETTINGS
        -- A budget of 0 starves background TTL merges, leaving the OPTIMIZE below as the only
        -- merge: a second one rebuilds the entry from the merged infos and would hide the bug.
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_group_by_all_future;

    INSERT INTO t_ttl_patch_group_by_all_future SELECT number, now() - INTERVAL 2 DAY, number FROM numbers(100);

    -- Every row, not just some of them.
    UPDATE t_ttl_patch_group_by_all_future SET event_time = now() + INTERVAL 5 DAY WHERE TRUE
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    -- TTL merges stay enabled, so the merge takes the ordinary path, not the blocked one.
    SYSTEM START MERGES t_ttl_patch_group_by_all_future;
    OPTIMIZE TABLE t_ttl_patch_group_by_all_future FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_all_future;"

# No row is due any more, so the bound must follow them into the future - not collapse to zero.
${CLICKHOUSE_CLIENT} -q "
    SELECT min(group_by_ttl_info.min[1]) > now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_group_by_all_future' AND active
      AND partition_id NOT LIKE 'patch-%';
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by_all_future;"

# -------------------------------------------------------------------
# Case 23: an unblocked patched merge leaves the GROUP BY entry schedulable
#
# When only some rows move forward the rebuilt bounds look right, and the pre-patch
# `ttl_finished` the algorithm carried over is the only thing that is wrong. No
# system.parts column shows it; the TTL selectors do, because they gate on
# hasAnyNonFinishedTTLs() before looking at the bound - so once the patched rows do
# fall due, nothing ever schedules the roll-up they need.
# -------------------------------------------------------------------
echo "-- Case 23: an unblocked patched merge leaves the GROUP BY entry schedulable"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_group_by_live
    (
        id UInt64,
        event_time DateTime,
        value UInt64
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time GROUP BY id SET value = max(value)
    SETTINGS
        -- As in Case 22, 0 keeps the OPTIMIZE the only merge; it is raised once that part exists.
        max_number_of_merges_with_ttl_in_pool = 0,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_group_by_live;

    -- Ten rows per key, all expired, so a roll-up that runs is visible as a row drop.
    INSERT INTO t_ttl_patch_group_by_live SELECT number % 10, now() - INTERVAL 2 DAY, number FROM numbers(100);

    -- A lightweight update moves two keys' worth of rows just past the merge.
    UPDATE t_ttl_patch_group_by_live SET event_time = now() + INTERVAL 10 SECOND WHERE id < 2
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM START MERGES t_ttl_patch_group_by_live;
    OPTIMIZE TABLE t_ttl_patch_group_by_live FINAL;
"

# Eighty expired rows collapse to eight; the twenty the patch moved forward pass through.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_live;"

# Those twenty fall due 10 seconds after the UPDATE, so wait past that deadline once before
# opening the pool - the retries below only absorb a slow merge, not the wait.
sleep 11
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ttl_patch_group_by_live MODIFY SETTING max_number_of_merges_with_ttl_in_pool = 100;"

# They must now roll up to one row per key. A finished entry hides the part from
# TTLRowDeleteMergeSelector::canConsiderPart and the count sits at 28 forever; a visible one only
# needs the pool to get to it, which under sanitizers can take minutes - poll long, break early.
for _ in $(seq 1 120); do
    live_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_live;")
    [[ "$live_rows" -lt 28 ]] && break
    sleep 1
done
echo "rolled up after the unblocked patched merge: $([[ "$live_rows" -lt 28 ]] && echo 1 || echo 0)"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by_live;"
