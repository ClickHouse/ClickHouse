#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
# Tag no-parallel: uses the server-wide `mt_select_parts_to_mutate_no_free_threads` failpoint and
# then relies on the pending mutation materializing after it is disabled; a concurrent test toggling
# the same global failpoint could postpone that mutation and make the final read flaky.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80648

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# The race is deterministic here, not probabilistic. The `mt_select_parts_to_mutate_no_free_threads`
# failpoint keeps the RENAME mutation from ever being selected, so the source parts stay physically
# at the old column name while the metadata already carries the new one. `alter_sync = 0` returns
# right after the metadata commit, and OPTIMIZE FINAL then merges the still-old parts under the new
# metadata, applying the rename on-fly. That is exactly the window that dropped the renamed column.
# The failpoint is disabled afterwards so the pending mutation can materialize before the final read
# (reading the renamed column while its mutation is still pending returns NULL regardless of the fix,
# because the merged part physically holds the new name while the read maps new->old). Same idiom as
# 03830_vertical_merge_inject_column_after_drop.

# The final SELECT is only meaningful after the pending RENAME COLUMN mutation has materialized:
# reading the renamed column while its mutation is still pending returns NULL regardless of the
# fix. If the mutation does not finish within the budget (for example on a very busy worker),
# report it as a timeout (print the still-pending rows and return 1) so it surfaces as such
# instead of falling through to the pre-materialization NULL, which would look like a false
# data-loss regression.
wait_mutation() {
    local table="$1"
    for _ in $(seq 1 200); do
        [ "$(${CLICKHOUSE_CLIENT} --query="SELECT min(is_done) FROM system.mutations WHERE database = currentDatabase() AND table = '${table}'")" = "1" ] && return 0
        sleep 0.3
    done
    echo "TIMEOUT: RENAME COLUMN mutation on ${table} did not finish; still-pending mutations:"
    ${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = '${table}' AND is_done = 0 FORMAT Vertical"
    return 1
}

# mt_select_parts_to_mutate_no_free_threads is server-global; clear it on every exit path so a
# failure under `set -e` before an in-flow disable cannot leave it armed for the rest of the run.
# The in-flow disables stay: the pending mutation only materializes once the failpoint is off.
disable_failpoint() {
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
}
trap disable_failpoint EXIT

# Phase 1: a column WITH a default (String DEFAULT ''). This is the facet #104822 fixed:
# a dropped-and-refilled default keeps the row count but the original values must survive.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_merge_race;
    CREATE TABLE t_rename_merge_race (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_merge_race VALUES (1, 'hello'), (2, 'world');
    INSERT INTO t_rename_merge_race VALUES (3, 'foo'), (4, 'bar');
    INSERT INTO t_rename_merge_race VALUES (5, 'baz'), (6, 'qux');
"

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race RENAME COLUMN d TO d1 SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race FINAL"
disable_failpoint
wait_mutation t_rename_merge_race

count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t_rename_merge_race WHERE d1 != ''")
if [ "$count" != "6" ]; then
    echo "FAIL (String DEFAULT ''): expected 6 non-empty rows, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, d1 FROM t_rename_merge_race ORDER BY id"
    exit 1
fi

# Phase 2: a Dynamic column with NO default (the residual facet this fix closes). The merge sees the
# new name in metadata, finds it absent from the parts (they still hold the old name), and without
# the fix expires and drops it from the merged part, so every value reads back as NULL. A default
# would have refilled it (Phase 1); a Dynamic column has nothing to refill from.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_merge_race_dynamic;
    SET allow_experimental_dynamic_type = 1;
    CREATE TABLE t_rename_merge_race_dynamic (x UInt64, y UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_merge_race_dynamic SELECT number, number FROM numbers(3);
    ALTER TABLE t_rename_merge_race_dynamic ADD COLUMN d Dynamic SETTINGS mutations_sync = 1;
    INSERT INTO t_rename_merge_race_dynamic SELECT number, number, number FROM numbers(3, 3);
    INSERT INTO t_rename_merge_race_dynamic SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
    INSERT INTO t_rename_merge_race_dynamic SELECT number, number, NULL FROM numbers(9, 3);
    INSERT INTO t_rename_merge_race_dynamic SELECT number, number, multiIf(number % 3 = 0, number, number % 3 = 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
"

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_dynamic RENAME COLUMN d TO d1 SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race_dynamic FINAL"
disable_failpoint
wait_mutation t_rename_merge_race_dynamic

# 8 of the 15 rows hold a non-null Dynamic value. Before the fix they read back as NULL, dropping
# this count to 0.
count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t_rename_merge_race_dynamic WHERE d1 IS NOT NULL SETTINGS allow_experimental_dynamic_type = 1")
if [ "$count" != "8" ]; then
    echo "FAIL (Dynamic no-default): expected 8 non-null rows, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT x, d1 FROM t_rename_merge_race_dynamic ORDER BY x SETTINGS allow_experimental_dynamic_type = 1"
    exit 1
fi

# Phase 3: the old name is re-added before the pending RENAME materializes. RENAME COLUMN a -> b is
# a barrier, but a later non-barrier ADD COLUMN a does not wait for it, so the metadata can carry
# both a and b while the parts still physically store only the pre-rename a. The merge must keep the
# physical a bound to b only (its rename target) and default the re-added a; it must not bind the old
# bytes to both names. Doing so would make the merged part carry both a and b off one physical column
# and the pending rename mutation, finding b already present, aborts with a LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_merge_race_reuse;
    CREATE TABLE t_rename_merge_race_reuse (id UInt64, a String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_merge_race_reuse VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');
"

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_reuse RENAME COLUMN a TO b SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_reuse ADD COLUMN a String DEFAULT 'reused_default' SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race_reuse FINAL"
disable_failpoint
wait_mutation t_rename_merge_race_reuse

# b must carry the original values (renamed old column); the re-added a must be its default for every
# row, never the old bytes.
count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t_rename_merge_race_reuse WHERE b = ['AAA', 'BBB', 'CCC'][id] AND a = 'reused_default'")
if [ "$count" != "3" ]; then
    echo "FAIL (rename target reused): expected 3 rows with renamed b and defaulted a, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, a, b FROM t_rename_merge_race_reuse ORDER BY id"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_rename_merge_race"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_rename_merge_race_dynamic"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_rename_merge_race_reuse"

# Phase 4: mixed generations. Same setup as Phase 3, but a fresh insert lands after the RENAME and
# the re-ADD, so a new part already physically stores b (and the re-added a) while the old part
# still stores only the pre-rename a. A single merge can never see such a mix: parts on different
# sides of a pending mutation have different current mutation versions and every merge predicate
# refuses to merge them, so OPTIMIZE FINAL must be refused while the rename is pending. Once the
# rename materializes, the merge must combine both generations without misbinding the old physical
# a bytes to the re-added a.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_merge_race_mixed;
    CREATE TABLE t_rename_merge_race_mixed (id UInt64, a String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_rename_merge_race_mixed VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');
"

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_mixed RENAME COLUMN a TO b SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_mixed ADD COLUMN a String DEFAULT 'reused_default' SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_rename_merge_race_mixed (id, b, a) VALUES (4, 'DDD', 'ddd')"

# The failpoint pins the pending mutation only for plain MergeTree; the suite may run with the
# table converted to ReplicatedMergeTree, where the rename can materialize at any moment, so the
# refusal is asserted only for the plain engine.
engine=$(${CLICKHOUSE_CLIENT} --query="SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_rename_merge_race_mixed'")
if [ "$engine" = "MergeTree" ]; then
    optimize_error=$(${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race_mixed FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 || true)
    if ! echo "$optimize_error" | grep -q "CANNOT_ASSIGN_OPTIMIZE"; then
        echo "FAIL (mixed generations): expected OPTIMIZE FINAL to be refused while the rename is pending, got: $optimize_error"
        exit 1
    fi
fi

disable_failpoint
wait_mutation t_rename_merge_race_mixed
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race_mixed FINAL"

count=$(${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM t_rename_merge_race_mixed
    WHERE (id <= 3 AND b = ['AAA', 'BBB', 'CCC'][id] AND a = 'reused_default')
       OR (id = 4 AND b = 'DDD' AND a = 'ddd')")
if [ "$count" != "4" ]; then
    echo "FAIL (mixed generations): expected 4 correct rows after the rename materialized, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, a, b FROM t_rename_merge_race_mixed ORDER BY id"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_rename_merge_race_mixed"

# Phase 5: a patch part (lightweight update) on the rename target. Same setup as Phase 3, plus a
# lightweight UPDATE of b after the RENAME and the re-ADD, so a patch part carrying the new value of
# b exists while the source parts still physically store only the pre-rename a. The merge under the
# pending rename must not consume (and thereby not lose) that patch: patch selection is version-gated
# in getPatchesToApplyOnMerge, so a post-rename patch is never applied to a merge over pre-rename
# parts; it stays alive, is applied on read, and materializes only after the rename does.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_rename_merge_race_patch;
    CREATE TABLE t_rename_merge_race_patch (id UInt64, a String)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;
    INSERT INTO t_rename_merge_race_patch VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');
"

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_patch RENAME COLUMN a TO b SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_rename_merge_race_patch ADD COLUMN a String DEFAULT 'reused_default' SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 --query="UPDATE t_rename_merge_race_patch SET b = 'patched' WHERE id = 2"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_rename_merge_race_patch FINAL"
disable_failpoint
wait_mutation t_rename_merge_race_patch

# The patched row must keep the lightweight update, the other rows their original (renamed) values,
# and the re-added a its default everywhere.
count=$(${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM t_rename_merge_race_patch
    WHERE b = if(id = 2, 'patched', ['AAA', 'BBB', 'CCC'][id]) AND a = 'reused_default'")
if [ "$count" != "3" ]; then
    echo "FAIL (patch part on rename target): expected 3 rows with the patched b preserved, got $count"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, a, b FROM t_rename_merge_race_patch ORDER BY id"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_rename_merge_race_patch"
echo "OK"
