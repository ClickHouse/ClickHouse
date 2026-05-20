#!/usr/bin/env bash
# Tags: race, no-flaky-check
# no-flaky-check: This test deliberately provokes logical errors via concurrent
# ALTER and INSERT. Re-running it many times under sanitizers blows the 180s
# per-test budget and reliably triggers an exception in debug builds, which is the bug being documented.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"' EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03518/alter_table', 'r1') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

# `timeout 30s` caps each query so a single hung ALTER/INSERT on a slow shard
# cannot keep the loop alive until the 600s per-test budget is hit.
function report_error()
{
    local STMT="$1"
    local RC="$2"
    local ERROR="$3"

    if [[ "$RC" -eq 124 ]]
    then
        echo "TIMEOUT: $STMT"
        exit 1
    fi

    if [[ "$RC" -ne 0 && -z "${ERROR}" ]]
    then
        echo "CLIENT EXIT $RC WITH EMPTY STDERR: $STMT"
        exit 1
    fi

    if [[ -n "${ERROR}" \
        && ! "${ERROR}" =~ "You can retry this error" \
        && ! "${ERROR}" =~ "Please retry this query" ]]
    then
        echo "${ERROR}"
    fi
}

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        for STMT in \
            "ALTER TABLE alter_table ADD COLUMN IF NOT EXISTS $1 String DEFAULT '0'" \
            "ALTER TABLE alter_table MODIFY COLUMN IF EXISTS $1 UInt64" \
            "ALTER TABLE alter_table DROP COLUMN IF EXISTS $1"
        do
            OUTPUT=$(timeout 30s $CLICKHOUSE_CLIENT --query "$STMT" 2>&1) && RC=0 || RC=$?
            ERROR=${OUTPUT//$'\n'/ }
            report_error "$STMT" "$RC" "$ERROR"
        done
    done
}

function thread_insert()
{
    local STMT="INSERT INTO alter_table (a, b, c, d, e, f, g) SELECT rand(1), rand(2), rand(3), rand(4), rand(5), rand(6), rand(7) FROM numbers(1000)"
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        OUTPUT=$(timeout 30s $CLICKHOUSE_CLIENT -q "$STMT" 2>&1) && RC=0 || RC=$?
        ERROR=${OUTPUT//$'\n'/ }
        report_error "$STMT" "$RC" "$ERROR"
    done
}

TIMEOUT=10

pids=()
thread_alter h & pids+=("$!")
thread_insert  & pids+=("$!")
thread_alter i & pids+=("$!")
thread_insert  & pids+=("$!")

rc=0
for pid in "${pids[@]}"
do
    wait "$pid" || rc=1
done
exit "$rc"
