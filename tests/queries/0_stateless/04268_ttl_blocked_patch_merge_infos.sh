#!/usr/bin/env bash
# Tags: no-s3-storage, long
# Tag no-s3-storage -- mirrors 04267, whose blocked-patch cases these are; patch merges are
# exercised on local storage, and the sanitizer runtime of the four cases needs its own budget.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Blocked patched merges must rebuild or carry the part's TTL infos correctly. Split out of
# 04267: these four cases wait on real deadlines and the background pool, and under msan the
# combined file ran into the per-test timeout.

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
    CREATE TABLE t_ttl_rows_where_predicate (id UInt64, ts DateTime, flag UInt8, payload String)
    ENGINE = MergeTree()
    ORDER BY id
    TTL ts + INTERVAL 50 YEAR DELETE WHERE flag = 1
    SETTINGS
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    -- The oldest ts belongs to a flag = 0 row, which this rule can never delete, so it must not
    -- become the bound. Inserted before TTL merges are stopped so no background TTL pass can rewrite
    -- the part first; STOP TTL MERGES stops those merges, it does not block inserts.
    INSERT INTO t_ttl_rows_where_predicate VALUES (1, '2000-01-01 00:00:00', 0, 'a'), (3, '2030-01-01 00:00:00', 1, 'a');
    INSERT INTO t_ttl_rows_where_predicate VALUES (2, '2020-01-01 00:00:00', 1, 'a');

    -- A patch part is what puts the merge on the recalculation path at all: without one,
    -- recalculate_ttl_for_patches stays false, and a 50-year TTL is never due so the ordinary TTL
    -- step does not run either - the merged infos would then just be folded source metadata and the
    -- check below would pass on an unfixed build. This update deliberately touches neither ts nor
    -- flag, so the expected bound is unchanged by it.
    UPDATE t_ttl_rows_where_predicate SET payload = 'b' WHERE id = 1
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_rows_where_predicate;
    OPTIMIZE TABLE t_ttl_rows_where_predicate FINAL;
"

# 2070 is the oldest flag = 1 row's TTL; folding the flag = 0 row in would give 2050.
# `rows = 3` selects the merged data part: the lightweight update above leaves a patch part active
# too, and it carries no rows-WHERE entry of its own.
${CLICKHOUSE_CLIENT} -q "
    SELECT rows_where_ttl_info.min[1] = toDateTime('2020-01-01 00:00:00') + INTERVAL 50 YEAR
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_rows_where_predicate' AND active AND rows = 3;
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

# Only `ttl_finished` is carried across; the bound itself is rebuilt, so the ten rows patched six
# days out move it into the future while the ninety still-expired ones keep the part due via `min`.
${CLICKHOUSE_CLIENT} -q "
    SELECT max(group_by_ttl_info.max[1]) > now(), min(group_by_ttl_info.min[1]) < now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_group_by' AND active
      AND partition_id NOT LIKE 'patch-%';
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_group_by;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by;"

# Case 20: the carried GROUP BY entry leaves the part schedulable
#
# Case 19 pins the bound the carried entry holds. The other half of that contract
# is invisible to it: the entry must also stay unfinished, because the TTL
# selectors gate on hasAnyNonFinishedTTLs() before they ever look at the bound
# (TTLRowDeleteMergeSelector::canConsiderPart). Combining the source parts already
# strips ttl_finished, so the carried entry arrives unfinished and a later TTL
# merge still picks the part up. Only a background merge exercises that gate -
# OPTIMIZE bypasses the selector - so this rolls up on the pool and watches rows.
# -------------------------------------------------------------------
echo "-- Case 20: the carried GROUP BY entry leaves the part schedulable"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_group_by_sched
    (
        id UInt64,
        event_time DateTime,
        value UInt64
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY GROUP BY id SET value = max(value)
    SETTINGS
        -- Compared against the SERVER-wide running TTL-merge count (StorageMergeTree), so a
        -- low budget starves this table whenever sibling tests hold the slot; the gate under
        -- test is the selector's, not the budget.
        max_number_of_merges_with_ttl_in_pool = 100,
        merge_with_ttl_timeout = 0,
        apply_patches_on_merge = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_patch_group_by_sched;

    -- Ten rows per key, so a rollup that actually runs is visible as a row drop.
    INSERT INTO t_ttl_patch_group_by_sched SELECT number % 10, now() - INTERVAL 2 DAY, number FROM numbers(100);

    UPDATE t_ttl_patch_group_by_sched SET event_time = now() + INTERVAL 5 DAY WHERE id < 2
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_patch_group_by_sched;
    SYSTEM START MERGES t_ttl_patch_group_by_sched;
    OPTIMIZE TABLE t_ttl_patch_group_by_sched FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_sched;"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_group_by_sched;"

# The eighty still-expired rows collapse to one per key; the twenty patched into the
# future stay. A part hidden from the selector would sit at 100 forever; a visible one only
# needs the pool to get to it, which under sanitizers can take minutes - poll long, break early.
sleep 11
for _ in $(seq 1 120); do
    sched_rows=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_sched;")
    [[ "$sched_rows" -lt 100 ]] && break
    sleep 1
done
echo "rolled up after the blocked patched merge: $([[ "$sched_rows" -lt 100 ]] && echo 1 || echo 0)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by_sched;"

# Case 21: a patch moving EVERY row into the future must move the bound with them
#
# Carrying the pre-patch entry verbatim left a `max` no row satisfied any more. The next real TTL
# merge seeds `ttl_finished` from that expired `max` (TTLAggregationAlgorithm), finds nothing to
# roll up, and finalizes the rule as finished - after which the part is never selected for this
# GROUP BY again and the rows overstay once their real deadline arrives.
# -------------------------------------------------------------------
echo "-- Case 21: a patch moving every row into the future moves the bound too"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_patch_group_by_future
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

    SYSTEM STOP MERGES t_ttl_patch_group_by_future;

    INSERT INTO t_ttl_patch_group_by_future SELECT number, now() - INTERVAL 2 DAY, number FROM numbers(100);

    -- Every row, not just some of them.
    UPDATE t_ttl_patch_group_by_future SET event_time = now() + INTERVAL 5 DAY WHERE TRUE
    SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

    SYSTEM STOP TTL MERGES t_ttl_patch_group_by_future;
    SYSTEM START MERGES t_ttl_patch_group_by_future;
    OPTIMIZE TABLE t_ttl_patch_group_by_future FINAL;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_patch_group_by_future;"

# No row is due any more, so neither end of the bound may still sit in the past.
${CLICKHOUSE_CLIENT} -q "
    SELECT min(group_by_ttl_info.min[1]) > now()
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_patch_group_by_future' AND active
      AND partition_id NOT LIKE 'patch-%';
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES t_ttl_patch_group_by_future;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_patch_group_by_future;"
