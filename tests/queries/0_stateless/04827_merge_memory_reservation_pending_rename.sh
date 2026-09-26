#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the JSON type is not available in the fast test.
#
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a merge that runs with a pending, not yet materialized RENAME COLUMN. The rename is applied on-fly at read
# time: the merged metadata already carries the new name while the source parts still physically store the old
# one, and MergeTask keeps the rename target alive (reading the old column through AlterConversions) instead of
# expiring it. The estimate used to derive presence purely from the current on-disk column names, so it priced
# such a column as expired - none of its read/write buffers entered the reservation while the real merge read
# and rewrote it. It now mirrors the same rename map and probes the source parts under the old name.
#
# The mt_select_parts_to_mutate_no_free_threads failpoint keeps the RENAME mutation unselected while OPTIMIZE
# merges the parts (the same window as 04648_alter_rename_column_no_default_merge_race). OPTIMIZE reserves
# unconditionally, so under a pathologically small soft limit the merge must still run to a single part, and
# the merged part must keep the renamed columns' data - a merge that wrongly expired them would drop the
# values for good (the columns carry no default to refill them from).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Phase 1: wide JSON and Dynamic columns as pending rename targets of a plain merge.
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_pending_rename
    (
        k UInt64,
        j JSON,
        d Dynamic
    )
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_pending_rename;
    INSERT INTO t_merge_mem_pending_rename
        SELECT number, toJSONString(map('a', repeat('x', 50), 'n', toString(number))), number
        FROM numbers(1000);
    INSERT INTO t_merge_mem_pending_rename
        SELECT number, toJSONString(map('b', repeat('y', 50))), 'str_' || toString(number)
        FROM numbers(1000, 1000);

    -- Keep the RENAME mutation unselected, so the parts still store j / d while the metadata
    -- already carries j1 / d1 when the merge below prices and runs. Both renames go into ONE
    -- ALTER: a rename is a barrier command, and a second ALTER would synchronously wait for the
    -- first (never-selected) mutation regardless of alter_sync.
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_merge_mem_pending_rename RENAME COLUMN j TO j1, RENAME COLUMN d TO d1 SETTINGS alter_sync = 0;
    SYSTEM START MERGES t_merge_mem_pending_rename;

    OPTIMIZE TABLE t_merge_mem_pending_rename FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_pending_rename;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_pending_rename' AND active;
    -- The merge must keep the rename targets' data: both columns are written into the merged part under
    -- their new names, with at least the payload's worth of bytes (not rewritten as empty defaults).
    SELECT
        sumIf(column_data_uncompressed_bytes, column = 'j1') >= 50000,
        sumIf(column_data_uncompressed_bytes, column = 'd1') >= 1000
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_merge_mem_pending_rename' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

# Phase 2: the rename target's only live values sit in a patch part (apply_patches_on_merge), the facet the
# on-fly rename recovery exists for - ADD COLUMN, a lightweight UPDATE into it, then a pending RENAME. The
# merge must neither expire the target (its values live only in the patch) nor lose the patch, and the
# reservation must price the patch-part readers and the renamed column's writer streams the same way.
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_pending_rename_patch (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_pending_rename_patch;
    INSERT INTO t_merge_mem_pending_rename_patch VALUES (1, 'x'), (2, 'y');
    INSERT INTO t_merge_mem_pending_rename_patch VALUES (3, 'z'), (4, 'w');
    ALTER TABLE t_merge_mem_pending_rename_patch ADD COLUMN a String SETTINGS mutations_sync = 1;

    SET enable_lightweight_update = 1;
    UPDATE t_merge_mem_pending_rename_patch SET a = 'patched_payload' WHERE id = 2;

    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_merge_mem_pending_rename_patch RENAME COLUMN a TO b SETTINGS alter_sync = 0;
    SYSTEM START MERGES t_merge_mem_pending_rename_patch;

    OPTIMIZE TABLE t_merge_mem_pending_rename_patch FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_pending_rename_patch;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_pending_rename_patch' AND active AND startsWith(name, 'all_');
    -- The merged part keeps the patch-only column under its new name, with more than four empty strings'
    -- worth of data (the patched value survived).
    SELECT sumIf(column_data_uncompressed_bytes, column = 'b') >= 10
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_merge_mem_pending_rename_patch' AND active AND startsWith(name, 'all_');
" -- --merges_mutations_memory_usage_soft_limit=1
