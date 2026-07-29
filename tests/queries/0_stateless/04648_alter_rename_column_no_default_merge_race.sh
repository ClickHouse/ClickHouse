#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80648, the facet where the
# renamed column has no default expression, so a merge that wrongly expires it loses the values for
# good.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# The window requires the RENAME mutation to stay unselected while `OPTIMIZE ... FINAL` merges the
# parts, so the parts still store the old column name while the metadata already carries the new
# one. `mt_select_parts_to_mutate_no_free_threads` is the only mechanism that opens it: `SYSTEM STOP
# MERGES` aborts the explicit OPTIMIZE too ("Cancelled merging parts"), and the pool-size thresholds
# are bypassed on an idle server by the `occupied <= 1` short circuit in
# CompactionStatistics::getMaxSourcePartBytesForMutation.
#
# The failpoint is server-global, but no assertion waits for the mutation, so a concurrent test
# toggling it cannot make this test fail: `system.parts_columns` records whether the merge kept a
# physical column for the rename target, which is decided by the merge itself and is stable from the
# moment OPTIMIZE returns. If the failpoint is cleared mid-window the rename materializes early and
# the merge no longer sees the race, which costs coverage for that run but cannot turn a correct
# server red. Same trade-off as 03830_vertical_merge_inject_column_after_drop, which is untagged.
disable_failpoint() {
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
}
trap disable_failpoint EXIT

# `expired` is the check: for a column with no default, dropping it from the merged part is the data
# loss - there is nothing left to refill it from.
assert_kept() {
    local table="$1" column="$2" label="$3"
    local expired
    expired=$(${CLICKHOUSE_CLIENT} --query="
        SELECT countIf(column = '${column}') = 0 FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active")
    if [ "$expired" != "0" ]; then
        echo "FAIL (${label}): the merge expired ${column}, so its values are lost"
        ${CLICKHOUSE_CLIENT} --query="
            SELECT name, groupArray(column) FROM system.parts_columns
            WHERE database = currentDatabase() AND table = '${table}' AND active GROUP BY name ORDER BY name"
        exit 1
    fi
}

# Phase 1: a plain column with no default at all.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default;
    CREATE TABLE t_rename_no_default (id UInt64, d String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_no_default VALUES (1, 'hello'), (2, 'world');
    INSERT INTO t_rename_no_default VALUES (3, 'foo'), (4, 'bar');
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_rename_no_default RENAME COLUMN d TO d1 SETTINGS alter_sync = 0;
    OPTIMIZE TABLE t_rename_no_default FINAL;
"
assert_kept t_rename_no_default d1 "String, no default"
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default"

# Phase 2: the column is Dynamic. Any type reproduces the loss; Dynamic is kept because it cannot
# carry a default expression, so it is a type where the bug is unavoidable rather than observable.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default_dynamic;
    SET allow_experimental_dynamic_type = 1;
    CREATE TABLE t_rename_no_default_dynamic (x UInt64, y UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_no_default_dynamic SELECT number, number FROM numbers(3);
    ALTER TABLE t_rename_no_default_dynamic ADD COLUMN d Dynamic SETTINGS mutations_sync = 1;
    INSERT INTO t_rename_no_default_dynamic SELECT number, number, number FROM numbers(3, 3);
    INSERT INTO t_rename_no_default_dynamic SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
    INSERT INTO t_rename_no_default_dynamic SELECT number, number, NULL FROM numbers(9, 3);
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_rename_no_default_dynamic RENAME COLUMN d TO d1 SETTINGS alter_sync = 0;
    OPTIMIZE TABLE t_rename_no_default_dynamic FINAL;
"
assert_kept t_rename_no_default_dynamic d1 "Dynamic, no default"
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_dynamic"

# Phase 3: the opposite direction - the keep-alive must not over-apply. The old name is re-added
# before the pending rename materializes, so the metadata carries both a and b while the parts still
# store only the pre-rename a. That physical a belongs to b once the rename materializes, so the
# merge must keep it under its own name and must not also claim b as present: doing so would bind one
# set of bytes to two logical columns.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default_reuse;
    CREATE TABLE t_rename_no_default_reuse (id UInt64, a String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_no_default_reuse VALUES (1, 'AAA'), (2, 'BBB');
    INSERT INTO t_rename_no_default_reuse VALUES (3, 'CCC'), (4, 'DDD');
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_rename_no_default_reuse RENAME COLUMN a TO b SETTINGS alter_sync = 0;
    ALTER TABLE t_rename_no_default_reuse ADD COLUMN a String DEFAULT 'reused_default' SETTINGS alter_sync = 0;
    OPTIMIZE TABLE t_rename_no_default_reuse FINAL;
"
cols=$(${CLICKHOUSE_CLIENT} --query="
    SELECT arraySort(groupArray(DISTINCT column)) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_rename_no_default_reuse' AND active
      AND column IN ('a', 'b')")
if [ "$cols" != "['a']" ]; then
    echo "FAIL (rename target reused): the merged part should physically hold only a, got $cols"
    exit 1
fi
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_reuse"

# Phase 4: a column whose only live values are in a patch part. `ADD COLUMN a` plus a lightweight
# `UPDATE` leaves every base part without a physical a, so the merge must not expire it - neither
# under its own name nor as a pending rename target - otherwise the patch is never requested and the
# updated value is silently lost. The own-name case needs no failpoint, so it asserts the values.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default_patch;
    CREATE TABLE t_rename_no_default_patch (id UInt64, v String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;
    SYSTEM STOP MERGES t_rename_no_default_patch;
    INSERT INTO t_rename_no_default_patch VALUES (1, 'x'), (2, 'y');
    INSERT INTO t_rename_no_default_patch VALUES (3, 'z'), (4, 'w');
    ALTER TABLE t_rename_no_default_patch ADD COLUMN a String SETTINGS mutations_sync = 1;
"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 --query="UPDATE t_rename_no_default_patch SET a = 'patched' WHERE id = 2"
${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES t_rename_no_default_patch"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_no_default_patch FINAL"

count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t_rename_no_default_patch WHERE a = if(id = 2, 'patched', '')")
if [ "$count" != "4" ]; then
    echo "FAIL (patch-only column, own name): expected 4 rows with the patched a preserved, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, v, a FROM t_rename_no_default_patch ORDER BY id"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_patch"

# Phase 5: the same patch-only column, but as the target of a pending rename.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default_patch_rename;
    CREATE TABLE t_rename_no_default_patch_rename (id UInt64, v String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;
    SYSTEM STOP MERGES t_rename_no_default_patch_rename;
    INSERT INTO t_rename_no_default_patch_rename VALUES (1, 'x'), (2, 'y');
    INSERT INTO t_rename_no_default_patch_rename VALUES (3, 'z'), (4, 'w');
    ALTER TABLE t_rename_no_default_patch_rename ADD COLUMN a String SETTINGS mutations_sync = 1;
"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 --query="UPDATE t_rename_no_default_patch_rename SET a = 'patched' WHERE id = 2"
${CLICKHOUSE_CLIENT} --query="
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_rename_no_default_patch_rename RENAME COLUMN a TO b SETTINGS alter_sync = 0;
    SYSTEM START MERGES t_rename_no_default_patch_rename;
    OPTIMIZE TABLE t_rename_no_default_patch_rename FINAL;
"
assert_kept t_rename_no_default_patch_rename b "patch-only column, rename target"
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_patch_rename"

echo "OK"
