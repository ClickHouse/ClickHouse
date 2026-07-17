#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree, no-replicated-database
# - no-parallel: the mutation-execution pause failpoint is process-global; while enabled it would
#   stall any concurrent mutation in other tests until this test disables it.
# - no-shared-merge-tree, no-replicated-database: this targets the plain MergeTree mutation path and
#   `StorageMergeTree::killMutation`; the Shared/Replicated engines cancel mutations differently.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `KILL MUTATION` must stop a mutation that is already in its execution phase (the
# new part is being written). Previously an operator precedence bug in
# `MutationContext::checkOperationIsNotCanceled` made the `is_cancelled` flag set by `KILL MUTATION`
# be ignored once the new part had been created, so a running mutation could not be killed and ran
# to completion.
#
# The mutation is parked deterministically inside its execution phase with the
# `mt_mutate_task_pause_in_execution` failpoint (which sits in the mutating write loop, after the new
# part is created), instead of driving a huge slow mutation and racing the kill -- that timing-based
# form was slow and flaky, and timed out (> 180s) on the debug flaky-check lane.

FP=mt_mutate_task_pause_in_execution

on_exit() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_kill_mutation_execution" 2>/dev/null
}
trap on_exit EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_kill_mutation_execution (key UInt64, value UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0"

# A tiny part is enough: the failpoint, not the data size, holds the mutation in the execution phase.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_kill_mutation_execution SELECT number, number FROM numbers(1000)"

# Park the mutation in its execution phase, then submit it (mutations_sync=0 returns immediately).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --mutations_sync=0 -q "
    ALTER TABLE t_kill_mutation_execution UPDATE value = value + 1000000 WHERE 1"

# Block until the mutation thread is parked inside the execution phase (the new part already exists,
# so `checkOperationIsNotCanceled` takes the `new_data_part != nullptr` branch).
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
echo "reached execution phase"

# `KILL MUTATION` sets the `is_cancelled` flag and returns immediately (it does not wait for the
# paused mutation thread, so there is no deadlock).
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_mutation_execution'" > /dev/null
echo "killed"

# Release the mutation: the next `checkOperationIsNotCanceled` must observe the kill and abort.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

# The mutation must leave system.merges. With the fix it aborts at once; without the fix it ignores
# the kill and runs the UPDATE to completion.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_kill_mutation_execution' AND is_mutation")" -ne 0 ]; do
    sleep 0.2
    i=$((i + 1))
    if [ $i -gt 150 ]; then
        echo "Mutation is still executing 30 seconds after KILL MUTATION"
        exit 1
    fi
done

# The killed mutation must not have been applied: `value` keeps its original sum (0+1+...+999).
# Without the fix the mutation completes and every value grows by 1000000.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(value) FROM t_kill_mutation_execution"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_kill_mutation_execution"
