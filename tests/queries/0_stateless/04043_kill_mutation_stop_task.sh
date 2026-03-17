#!/usr/bin/env bash
# Tags: no-parallel, long
# Tag no-parallel: Fails due to failpoint intersection

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

function wait_for_mutation_done_or_killed()
{
    local table=$1
    local mutation_id=$2
    local database=$3
    database=${database:="${CLICKHOUSE_DATABASE}"}

    for _ in {1..300}
    do
        sleep 0.3
        res=$(${CLICKHOUSE_CLIENT} --query="SELECT min(is_done) FROM system.mutations WHERE database='$database' AND table='$table' AND mutation_id='$mutation_id'")
        if [[ $res -eq 1 || $res -eq "" ]]; then
            return
        fi
    done

    echo "Timed out while waiting for mutation to execute!"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.mutations WHERE database='$database' AND table like '$table' AND mutation_id='$mutation_id' AND is_done=0"
}

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_kill_mut_prec;
    CREATE TABLE t_kill_mut_prec (key UInt64, val UInt64)
    ENGINE = MergeTree ORDER BY key;
    INSERT INTO t_kill_mut_prec SELECT number, 0 FROM numbers(5);
"

# Enable failpoint that pauses in MutateTask::execute() NEED_EXECUTE state,
# AFTER prepare() has created new_data_part. This guarantees the buggy code path
# (ternary true-branch, which skips is_cancelled check) is active.
$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_execute;
    ALTER TABLE t_kill_mut_prec UPDATE val = 1 WHERE 1
    SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 0;
"

# Wait for the mutation to be in progress and paused at the failpoint.
# At this point, new_data_part is non-null (prepare completed).
wait_for_mutation_in_progress "t_kill_mut_prec" "mutation_2.txt"
# Sleep doesn't affect the correctness of the test, but it help to
# reduce the chance of test failure.
sleep 1

# Kill the mutation while it is paused. This sets is_cancelled=true on MergeListElement.
$CLICKHOUSE_CLIENT --format Null -q "
    KILL MUTATION WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_kill_mut_prec'
"

# Release the failpoint. The mutation resumes and checkOperationIsNotCanceled() runs.
# With the bug:  is_cancelled is not checked (ternary true-branch), mutation completes.
# With the fix:  is_cancelled is detected, mutation throws ABORTED, part is discarded.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_execute"

# Wait for the mutation task to finish (either complete or abort).
wait_for_mutation_done_or_killed "t_kill_mut_prec" "mutation_2.txt"

# If kill worked: data is unchanged, val = 0 for all rows, sum(val) = 0.
# If kill didn't work: mutation completed, val = 1, sum(val) = 5.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(val) FROM t_kill_mut_prec;
    SELECT name FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_kill_mut_prec' AND active = 1;
    DROP TABLE t_kill_mut_prec SYNC;
"
