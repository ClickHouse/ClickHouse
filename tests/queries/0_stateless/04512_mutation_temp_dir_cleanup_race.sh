#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: the failpoints pause every ReplicatedMergeTree mutation on the server
# Tag no-shared-merge-tree: the failpoints are in the ReplicatedMergeTree mutation task
# Tag no-replicated-database: additional replicas execute the same mutation

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

set -e

# Both failpoints are server-global and pause every `ReplicatedMergeTree` mutation, so they must be
# disabled even when the test fails in the middle: otherwise the next tests on the same server hang.
# Disabling a pauseable failpoint also resumes the threads that are currently paused at it.
function disable_failpoints()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_before_rename_part;
        SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_temporary_part_released;
    " ||:
}

trap disable_failpoints EXIT

# Scope all text_log reads below to this run of the test, so that stale rows from a previous run
# (e.g. under a fixed --database, where the logger names are the same) cannot satisfy the checks.
start_time=$($CLICKHOUSE_CLIENT --query "SELECT now64(6)")

# `temporary_directories_lifetime = 1` makes the cleanup thread consider the temporary directory of
# an in-flight mutation old enough to be removed, which is exactly what the test needs to check.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt (num UInt32, num2 UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt/', '1')
    ORDER BY num
    SETTINGS min_bytes_for_wide_part = 0,
             temporary_directories_lifetime = 1,
             cleanup_delay_period = 1,
             cleanup_delay_period_random_add = 0,
             max_cleanup_delay_period = 1;

    INSERT INTO rmt SELECT number, number + 1 FROM numbers(1000);
"

CLEANUP_ROWS="FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('$start_time', 6)
        AND logger_name LIKE '${CLICKHOUSE_DATABASE}.rmt%' AND message LIKE '%tmp_mut_%'
        AND (message LIKE '%is in use (by merge/mutation/INSERT)%' OR message LIKE '%Removing temporary directory%')"

# Every finished iteration of the cleanup thread logs this message, so it tells the test that the
# cleanup thread has actually looked at the data directory, instead of just waiting for a while.
CLEANUP_ITERATIONS="SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('$start_time', 6)
        AND logger_name = '${CLICKHOUSE_DATABASE}.rmt (CleanupThread)' AND message LIKE 'Scheduling next cleanup%'"

# Wait until the cleanup thread finishes at least $1 more iterations than it had at the moment of the call.
function wait_for_cleanup_iterations()
{
    local wanted=$1
    local before
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    before=$($CLICKHOUSE_CLIENT --query "$CLEANUP_ITERATIONS")

    for _ in {1..300}
    do
        sleep 0.3
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        if [[ $($CLICKHOUSE_CLIENT --query "$CLEANUP_ITERATIONS") -ge $((before + wanted)) ]]
        then
            return 0
        fi
    done

    echo "The cleanup thread did not run $wanted times" >&2
    return 1
}

# Phase 1: pause the mutation right before its temporary part directory is renamed to the persistent
# name. The directory is still owned by the mutation task, so the cleanup thread must skip it.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_before_rename_part;
    ALTER TABLE rmt RENAME COLUMN num2 TO foo2 SETTINGS alter_sync = 0;
"

wait_for_mutation_in_progress "rmt" "0000000000"
timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_before_rename_part PAUSE"

wait_for_cleanup_iterations 2

$CLICKHOUSE_CLIENT --query "
    SELECT
        countIf(message LIKE '%is in use (by merge/mutation/INSERT)%') > 0 AS kept,
        countIf(message LIKE '%Removing temporary directory%') AS removed
    $CLEANUP_ROWS;
"

# Phase 2: this is the window the fix is about. Let the mutation continue up to the point where the
# `TemporaryParts` guard has just been released by `mutate_task.reset()`, and hold it there while the
# cleanup thread makes a few more passes over the data directory.
#
# With the fix, the temporary directory has already been renamed to the persistent name under the
# guard, so there is nothing left for the cleanup thread to remove. With the previous ordering, where
# `transaction_ptr->renameParts()` ran after `mutate_task.reset()`, the directory is still there and
# no longer registered in `TemporaryParts`, so the cleanup thread removes it right from under the
# rename: `removed` below becomes non-zero and the checks at the end of the test fail.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_after_temporary_part_released;
    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_before_rename_part;
"

timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_after_temporary_part_released PAUSE"

wait_for_cleanup_iterations 2

$CLICKHOUSE_CLIENT --query "
    SELECT countIf(message LIKE '%Removing temporary directory%') AS removed_after_release
    $CLEANUP_ROWS;

    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_after_temporary_part_released;
"

wait_for_mutation "rmt" "0000000000"

# The mutated part must be complete: every file listed in its checksums has to exist on disk.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM rmt WHERE foo2 % 1000 > 0;
    CHECK TABLE rmt SETTINGS check_query_single_value_result = 1;
    DROP TABLE rmt SYNC;
"
