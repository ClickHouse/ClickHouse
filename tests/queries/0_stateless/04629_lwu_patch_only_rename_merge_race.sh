#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
# Tag no-parallel: uses the server-wide `mt_select_parts_to_mutate_no_free_threads` failpoint and
# then relies on the pending RENAME mutation materializing after it is disabled; a concurrent test
# toggling the same global failpoint could postpone that mutation and make the final read flaky.
# Tag no-random-settings, no-random-merge-tree-settings: the regression requires a deterministic
# mutation/merge interleaving under the failpoint and a part layout where the updated column stays
# patch-only; randomized part-format/lifecycle settings would vary both. Randomized coverage of the
# fix mechanism is provided by 04628.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# The final SELECT is meaningful only after the pending RENAME mutation has materialized (reading the
# renamed column while it is still pending returns NULL regardless of the fix), so report a timeout
# explicitly instead of falling through to a false data-loss failure.
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

# The failpoint is server-global; clear it on every exit path so a failure under `set -e` cannot
# leave it armed for the rest of the run.
disable_failpoint() {
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
}
trap disable_failpoint EXIT

# A no-default column whose only live value lives in a patch part (lightweight UPDATE, never
# materialized) is renamed by a pending RENAME the merge straddles. The merge finds the new name
# absent from the source parts and, without the fix, expires and drops it while still claiming the
# patch version, so the updated value reads back as NULL after the rename materializes.
${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS t_patch_only_rename_race;
    CREATE TABLE t_patch_only_rename_race (id UInt64, value String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
             apply_patches_on_merge = 1, min_bytes_for_wide_part = 0;
    INSERT INTO t_patch_only_rename_race VALUES (1, 'a'), (2, 'b');
    INSERT INTO t_patch_only_rename_race VALUES (3, 'c');
    ALTER TABLE t_patch_only_rename_race ADD COLUMN acol Nullable(Int64);
"

${CLICKHOUSE_CLIENT} --query="UPDATE t_patch_only_rename_race SET acol = 42 WHERE id = 1" --enable_lightweight_update=1

${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE t_patch_only_rename_race RENAME COLUMN acol TO bcol SETTINGS alter_sync = 0"
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t_patch_only_rename_race FINAL SETTINGS optimize_throw_if_noop = 1"

# The value oracle alone can pass vacuously: if the merge skipped the patch, the result part
# would not claim the patch version and the later rename mutation would apply the still-live
# patch. Assert the merged (non-patch) part physically carries the column while the rename is
# still pending. Accept either name: in the DatabaseReplicated suite the failpoint does not pin
# the interleaving (it is a StorageMergeTree code path), so the merge may run before the rename
# registers and the merged part then carries acol instead of bcol.
merged_cols=$(${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_patch_only_rename_race'
      AND active AND column IN ('acol', 'bcol') AND part_name NOT LIKE 'patch-%'")
if [ "$merged_cols" != "1" ]; then
    echo "FAIL: expected exactly 1 active non-patch part column among acol/bcol after the merge, got '${merged_cols}'"
    ${CLICKHOUSE_CLIENT} --query="SELECT part_name, column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_patch_only_rename_race' AND active ORDER BY part_name, column"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_patch_only_rename_race"
    exit 1
fi

disable_failpoint
wait_mutation t_patch_only_rename_race

value=$(${CLICKHOUSE_CLIENT} --query="SELECT bcol FROM t_patch_only_rename_race WHERE id = 1")
if [ "$value" != "42" ]; then
    echo "FAIL: expected bcol = 42 for id = 1, got '${value}'"
    ${CLICKHOUSE_CLIENT} --query="SELECT id, bcol FROM t_patch_only_rename_race ORDER BY id"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_patch_only_rename_race"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_patch_only_rename_race"
echo "OK"
