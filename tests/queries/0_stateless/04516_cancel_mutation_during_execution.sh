#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: KILL MUTATION must stop a mutation that is already in its execution phase
# (rows are flowing through the mutating pipeline and the new part is being written).
# Previously, an operator precedence bug in MutationContext::checkOperationIsNotCanceled made
# the is_cancelled flag (set by KILL MUTATION) be ignored once the new part had been created,
# so a running mutation could not be killed and ran to completion.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_kill_mutation_execution"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_kill_mutation_execution (key UInt64, value UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0"

# A single part with 6 million rows.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_kill_mutation_execution SELECT number, number FROM numbers(6000000)
    SETTINGS max_block_size = 6000000, min_insert_block_size_rows = 6000000, min_insert_block_size_bytes = 1000000000"

# The mutation is slow in the execution phase: the updated column value contains sleepEachRow,
# so the sleeps happen while rows flow through the mutating pipeline (~120 seconds in total:
# 6 million rows * 20 microseconds). The WHERE condition is cheap, so the preliminary
# "how many rows are affected" check pays no sleeps. The sleep per block stays well below the
# 3 second limit of function_sleep_max_microseconds_per_block (65536 rows * 20 microseconds = 1.3 seconds).
$CLICKHOUSE_CLIENT --mutations_sync=0 -q "
    ALTER TABLE t_kill_mutation_execution
    UPDATE value = value + toUInt64(sleepEachRow(0.00002)) WHERE key >= 0"

# Wait until the mutation is in the execution phase: it is listed in system.merges and
# some rows have already flowed through the mutating pipeline.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation_execution' AND rows_read > 0")" -eq 0 ]; do
    sleep 0.5
    i=$((i + 1))
    if [ $i -gt 120 ]; then
        echo "Mutation did not reach the execution phase in 60 seconds"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation_execution'" > /dev/null
echo "killed"

# The executing mutation must disappear from system.merges quickly.
# Without the fix it kept running for the remaining ~2 minutes.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation_execution'")" -ne 0 ]; do
    sleep 0.5
    i=$((i + 1))
    if [ $i -gt 60 ]; then
        echo "Mutation is still executing 30 seconds after KILL MUTATION"
        exit 1
    fi
done

# The data must be intact.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(key), sum(value) FROM t_kill_mutation_execution"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_kill_mutation_execution"
