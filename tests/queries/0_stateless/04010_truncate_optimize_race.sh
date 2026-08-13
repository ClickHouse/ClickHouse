#!/usr/bin/env bash
# Tags: long, race

# Test for a race condition between TRUNCATE TABLE and OPTIMIZE TABLE FINAL
# with MergeTree transactions.
#
# The race: stopMergesAndWait (called by TRUNCATE) returns after the merge task
# completes, but the merge's implicit transaction may still roll back afterwards,
# reverting parts from merged state to pre-merge state. The empty part created
# by TRUNCATE then covers multiple pre-merge parts instead of the expected single
# merged part. This should be handled gracefully, not cause a LOGICAL_ERROR.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_truncate_optimize_race"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_truncate_optimize_race (x UInt64) ENGINE = MergeTree ORDER BY x"

# implicit_transaction wraps each query in a MergeTree transaction.
# When a merge's transaction rolls back, parts revert from merged to pre-merge state.
# throw_on_unsupported_query_inside_transaction allows DDL (TRUNCATE/OPTIMIZE) inside transactions.
SETTINGS="--implicit_transaction 1 --throw_on_unsupported_query_inside_transaction 0"

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO t_truncate_optimize_race VALUES (rand())" 2>/dev/null ||:
        $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO t_truncate_optimize_race VALUES (rand())" 2>/dev/null ||:
    done
}

function thread_optimize()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT $SETTINGS -q "OPTIMIZE TABLE t_truncate_optimize_race FINAL" 2>/dev/null ||:
    done
}

function thread_truncate()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT $SETTINGS -q "TRUNCATE TABLE t_truncate_optimize_race" 2>/dev/null ||:
    done
}

TIMEOUT=15

thread_insert &
thread_insert &
thread_optimize &
thread_optimize &
thread_truncate &
thread_truncate &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_optimize_race"

echo "OK"
