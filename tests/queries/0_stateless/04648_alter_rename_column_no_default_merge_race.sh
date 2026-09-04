#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80648, the facet where the
# renamed column has no default expression, so a merge that wrongly expires it loses the values for
# good.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# The window requires the RENAME mutation to stay unselected while `OPTIMIZE ... FINAL` merges the
# parts, so the parts still store the old column name while the metadata already carries the new
# one. `mt_select_parts_to_mutate_no_free_threads` is the only mechanism that opens it: the pool-size
# thresholds are bypassed on an idle server by the `occupied <= 1` short circuit in
# CompactionStatistics::getMaxSourcePartBytesForMutation, and `SYSTEM STOP MERGES` cannot substitute
# for it because it aborts the explicit OPTIMIZE too ("Cancelled merging parts").
#
# The failpoint is server-global, so a concurrent copy of this test can clear it mid-window. That
# only ever costs coverage here, never a red, because every assertion holds in both states: they
# read `system.parts_columns` for a column the merge had to keep either way. The
# remaining effect of an early release is that the rename may materialize on only some source parts,
# so OPTIMIZE legitimately refuses a mixed mutation version - `optimize_or_skip` below turns that one
# refusal into a skip. Same trade-off as 03830_vertical_merge_inject_column_after_drop, `no-parallel` too.
#
# Every assertion is about what a merge decided, so OPTIMIZE always runs with
# `optimize_throw_if_noop = 1`: a silently skipped merge would otherwise read as a lost column.
disable_failpoint() {
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
}
trap disable_failpoint EXIT

# A concurrent copy of this test clearing the server-global failpoint lets the pending rename
# materialize on only some of the source parts, and OPTIMIZE then legitimately refuses to merge parts
# with different mutation versions. Return non-zero for that one reason so the phase can skip, and
# keep failing on every other refusal - that is what `optimize_throw_if_noop = 1` is for.
optimize_or_skip() {
    local table="$1" err
    if err=$(${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE ${table} FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1); then
        return 0
    fi
    case "$err" in
        *"have different mutation version"*) return 1 ;;
    esac
    echo "FAIL (${table}): OPTIMIZE did not run: ${err}"
    exit 1
}

# For a column with no default, dropping it from the merged part is the data loss - there is nothing
# left to refill it from. Require both that the merged part still carries the column and that it
# carries at least `min_bytes` of data for it, so a column present but rewritten as empty defaults
# does not pass.
assert_kept() {
    local table="$1" column="$2" min_bytes="$3" label="$4"
    local kept
    kept=$(${CLICKHOUSE_CLIENT} --query="
        SELECT sumIf(column_data_uncompressed_bytes, column = '${column}') >= ${min_bytes}
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${table}' AND active")
    if [ "$kept" != "1" ]; then
        echo "FAIL (${label}): the merge did not keep the values of ${column}"
        ${CLICKHOUSE_CLIENT} --query="
            SELECT name, column, column_data_uncompressed_bytes FROM system.parts_columns
            WHERE database = currentDatabase() AND table = '${table}' AND active
            ORDER BY name, column"
        exit 1
    fi
}

# Phase 1: a plain column with no default at all.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_no_default;
    CREATE TABLE t_rename_no_default (id UInt64, d String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_no_default SELECT number, 'payload_value_' || toString(number) FROM numbers(500);
    INSERT INTO t_rename_no_default SELECT number, 'payload_value_' || toString(number) FROM numbers(500, 500);
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_rename_no_default RENAME COLUMN d TO d1 SETTINGS alter_sync = 0;
"
if optimize_or_skip t_rename_no_default; then
    assert_kept t_rename_no_default d1 5000 "String, no default"
fi
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
"
if optimize_or_skip t_rename_no_default_dynamic; then
    assert_kept t_rename_no_default_dynamic d1 1 "Dynamic, no default"
fi
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_dynamic"

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
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_no_default_patch FINAL SETTINGS optimize_throw_if_noop = 1"

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
"
# The threshold has to exceed what four empty strings occupy (9 bytes here), or preserving the target
# column while dropping the patch value still passes. The value itself cannot be asserted: logical b
# reads back empty until the rename materializes, so an exact-value check would fail on a correct
# server in exactly the state this phase exists to cover. Phase 4 asserts the value because it has no
# pending rename.
if optimize_or_skip t_rename_no_default_patch_rename; then
    assert_kept t_rename_no_default_patch_rename b 10 "patch-only column, rename target"
fi
disable_failpoint
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_rename_no_default_patch_rename"

echo "OK"
