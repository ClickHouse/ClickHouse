#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a DELETE racing another writer, one case per pair, ordered by the other writer.
#   1. vs merge, the DELETE parked: the merge retires the DELETE's target, so the partition is rescanned and the DELETE applies
#   2. vs mutation, the mutation parked: the DELETE's kill is moved onto the result with the source's other kills
#   3. vs insert, the DELETE parked: no conflict, both commit, the INSERT's rows are neither killed nor lost
#   4. vs DELETE, both parked: two overlapping DELETEs both commit, and each matched row is dead once
#   5. vs TRUNCATE, the DELETE parked: the target is gone, so the rescan finds nothing and the DELETE leaves no marker part
# no-parallel: the failpoints are server-wide.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1 --optimize_trivial_count_query 0 --optimize_use_implicit_projections 0"

DELETE_FP="unique_key_delete_pause_before_commit"
MERGE_FP="unique_key_merge_pause_before_commit"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $DELETE_FP" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MERGE_FP" 2>/dev/null || true
}
trap cleanup EXIT

delete_fp_enabled() {
    $CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$DELETE_FP'"
}

arm_delete_failpoint() {
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $DELETE_FP"
    if [[ $(delete_fp_enabled) != 1 ]]; then
        echo "$DELETE_FP did not arm"
        exit 1
    fi
}

# Returns once the DELETE DELETE_PID parks on the armed failpoint, or fails once it has exited or
# after 120s. The failpoint is PAUSEABLE_ONCE: the hit that parks a query also disables it, so
# `enabled` going from 1 to 0 marks this DELETE's hit even while an earlier one is still parked.
# `SYSTEM WAIT FAILPOINT ... PAUSE` cannot: a parked earlier DELETE already satisfies it.
wait_for_delete_to_park() {
    for _ in {1..240}; do
        if [[ $(delete_fp_enabled) == 0 ]]; then
            return 0
        fi
        if ! kill -0 "$DELETE_PID" 2>/dev/null; then
            return 1
        fi
        sleep 0.5
    done
    return 1
}

# Starts the DELETE `$1` in the background with its stderr in `$2` and returns once it parks.
start_parked_delete() {
    arm_delete_failpoint
    $CLICKHOUSE_CLIENT --query "$1" 2>"$2" >/dev/null &
    DELETE_PID=$!
    if ! wait_for_delete_to_park; then
        echo "the DELETE never reached $DELETE_FP"
        exit 1
    fi
}

ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_delete_err.txt"

# Releases the parked DELETE; prints `$1_ok 1` if it succeeded, else `$1_ok 0` and its error.
release_delete() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $DELETE_FP"
    if wait "$DELETE_PID"; then
        echo "$1_ok 1"
    else
        echo "$1_ok 0"
        cat "$ERR_FILE"
    fi
    rm -f "$ERR_FILE"
}

# 1. vs merge, the DELETE parked: red if the partition is not rescanned after the conflict
# (`delete_retried 0`), e.g. the commit skips the retired target instead of aborting, the retry
# gives up (`delete_ok 0`), or a row comes back twice (`still_unique` 0).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_vs_merge"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_del_vs_merge (id UInt32, v UInt32)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS min_bytes_for_wide_part = 0, parts_to_delay_insert = 10000, parts_to_throw_insert = 20000
"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_vs_merge"
for p in 0 1 2 3; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO uk_del_vs_merge SELECT number + ${p} * 250 AS id, ${p} AS v FROM numbers(250)
    "
done

start_parked_delete "DELETE FROM uk_del_vs_merge WHERE id < 250" "$ERR_FILE"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES uk_del_vs_merge"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE uk_del_vs_merge FINAL"
# Re-armed before the DELETE resumes, so the rescan after the conflict parks again.
arm_delete_failpoint
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $DELETE_FP"
if wait_for_delete_to_park; then
    echo "delete_retried 1"
else
    echo "delete_retried 0"
fi
release_delete delete

$CLICKHOUSE_CLIENT --query "
    SELECT 'merge_applied',
           count() = 750                 AS band_removed,
           countIf(id < 250) = 0         AS no_survivors,
           count() = countDistinct(id)   AS still_unique
    FROM uk_del_vs_merge
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_vs_merge"

# 2. vs mutation, the mutation parked: red if the late kill is not moved onto the result (`survivors`
# regains 0, 1, 2).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_mut_late SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_mut_late (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT --query "INSERT INTO uk_mut_late SELECT number, 'a' FROM numbers(0, 10)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MERGE_FP"

# alter_sync = 0, or the ALTER blocks on the pause the next line waits for.
$CLICKHOUSE_CLIENT --alter_sync 0 --query "ALTER TABLE uk_mut_late RENAME COLUMN v TO w"

$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM WAIT FAILPOINT $MERGE_FP PAUSE"

$CLICKHOUSE_CLIENT --query "DELETE FROM uk_mut_late WHERE id < 3"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MERGE_FP"
wait_for_all_mutations uk_mut_late

$CLICKHOUSE_CLIENT --query "
    SELECT 'late_kill_on_the_result', name, rows, unique_key_bitmap_versions FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_mut_late' AND active AND rows > 0
    ORDER BY name"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors', groupArray(id) FROM (SELECT id FROM uk_mut_late ORDER BY id)"


$CLICKHOUSE_CLIENT --query "DROP TABLE uk_mut_late SYNC"

# 3. vs insert, the DELETE parked: red if the DELETE fails (`insert_delete_ok 0`), kills a row
# committed after its snapshot (`expected_rows` 0), or leaves a key live twice (`every_id_unique` 0).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_vs_insert"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_del_vs_insert (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS min_bytes_for_wide_part = 0, parts_to_delay_insert = 10000, parts_to_throw_insert = 20000
"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_vs_insert"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_vs_insert SELECT number, 'old' FROM numbers(200)"

start_parked_delete "DELETE FROM uk_del_vs_insert WHERE id < 100" "$ERR_FILE"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_vs_insert SELECT number + 200, 'new' FROM numbers(50)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_vs_insert SELECT 500, 'new'"
release_delete insert_delete

# 200 original - 100 deleted + 50 new + 1 = 151.
$CLICKHOUSE_CLIENT --query "
    SELECT 'insert_applied',
           count() = 151                 AS expected_rows,
           countIf(id < 100) = 0         AS deleted_band_gone,
           countIf(v = 'new') = 51       AS concurrent_insert_kept,
           count() = countDistinct(id)   AS every_id_unique
    FROM uk_del_vs_insert
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_vs_insert"

# 4. vs DELETE, both parked: red if a read resolves only the newest bitmap
# version (one DELETE's rows in partition 0 come back). Partition 0 is one part holding both
# DELETEs' rows, neither set containing the other. The trivial count checks a dead row counts once.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_vs_del"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_del_vs_del (id UInt32, v UInt32)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id) PARTITION BY intDiv(id, 200)
    SETTINGS min_bytes_for_wide_part = 0
"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_vs_del"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_vs_del SELECT number, 0 FROM numbers(300)"

ERR_FILE_A="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_delete_a_err.txt"
ERR_FILE_B="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_delete_b_err.txt"

start_parked_delete "DELETE FROM uk_del_vs_del WHERE id < 150" "$ERR_FILE_A"
DELETE_A_PID=$DELETE_PID
start_parked_delete "DELETE FROM uk_del_vs_del WHERE id >= 100 AND id < 250" "$ERR_FILE_B"
DELETE_B_PID=$DELETE_PID

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $DELETE_FP"
DELETE_A_EXIT=0; wait "$DELETE_A_PID" || DELETE_A_EXIT=$?
DELETE_B_EXIT=0; wait "$DELETE_B_PID" || DELETE_B_EXIT=$?

echo -e "both_deletes_committed\t$DELETE_A_EXIT\t$DELETE_B_EXIT"
cat "$ERR_FILE_A" "$ERR_FILE_B"
rm -f "$ERR_FILE_A" "$ERR_FILE_B"

# 300 rows; the union of [0, 150) and [100, 250) is 250 dead, 50 live.
$CLICKHOUSE_CLIENT --query "
    SELECT 'delete_vs_delete', count(), countIf(id < 250), countIf(id >= 250) FROM uk_del_vs_del
"
$CLICKHOUSE_CLIENT --optimize_trivial_count_query 1 --query "
    SELECT 'delete_vs_delete_trivial', count() FROM uk_del_vs_del
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_vs_del"

# 5. vs TRUNCATE, the DELETE parked: red if the conflict fails the DELETE (`truncate_delete_ok 0`)
# or the aborted attempt leaves its marker part behind (`truncate_active_parts` 1).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_vs_truncate"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_del_vs_truncate (id UInt32, v UInt32)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id) PARTITION BY intDiv(id, 100)
    SETTINGS min_bytes_for_wide_part = 0
"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_vs_truncate"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_vs_truncate SELECT number, 0 FROM numbers(300)"

start_parked_delete "DELETE FROM uk_del_vs_truncate WHERE id >= 50 AND id < 150" "$ERR_FILE"
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE uk_del_vs_truncate"
release_delete truncate_delete

$CLICKHOUSE_CLIENT --query "SELECT 'truncate_rows', count() FROM uk_del_vs_truncate"
$CLICKHOUSE_CLIENT --query "
    SELECT 'truncate_active_parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_del_vs_truncate' AND active
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_vs_truncate"
