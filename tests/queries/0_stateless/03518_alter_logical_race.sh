#!/usr/bin/env bash
# Tags: race, zookeeper, no-flaky-check
# no-flaky-check: This test stresses concurrent ALTER and INSERT to guard against a
# column-resolution regression. It is expected to pass on master; it should fail only
# if that regression returns (a genuine bug surfaces as `NOT_FOUND_COLUMN_IN_BLOCK`,
# `DUPLICATE_COLUMN`, etc.). Re-running it many times under sanitizers and the thread
# fuzzer exceeds the per-test budget, which is why the flaky check is disabled.
#
# The ALTERs deliberately do NOT use `IF [NOT] EXISTS`: those clauses would suppress
# exactly the `NOT_FOUND_COLUMN_IN_BLOCK` / `DUPLICATE_COLUMN` errors this test exists
# to catch, making it pass trivially. Instead, when the server returns the documented
# retryable error under concurrent metadata ALTERs (`CANNOT_ASSIGN_ALTER`,
# "You can retry this error"), we re-run the same statement until it lands. Without that
# retry, a swallowed ADD would leave the table without the column and make the next
# MODIFY/DROP fail with NOT_FOUND_COLUMN_IN_BLOCK - a test artifact, not a server bug.
# So any error that survives retry is a real regression and fails the test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"' EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03518/alter_table', 'r1') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

# Run a single statement, retrying it on transient/retryable conditions until it
# actually lands (or until the time budget for this thread runs out). Retrying is what
# keeps each thread's ADD -> MODIFY -> DROP sequence consistent, so that any
# NOT_FOUND_COLUMN_IN_BLOCK / DUPLICATE_COLUMN that does appear is a genuine regression.
#
# `timeout 120s` caps each attempt so a single hung ALTER/INSERT on a slow shard cannot
# keep the loop alive until the 600s per-test budget is hit. The cap must stay generous:
# on a debug build with s3 storage under parallel load a single INSERT or ALTER
# occasionally takes more than 30 seconds without being hung.
function run_with_retry()
{
    local STMT="$1"

    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        OUTPUT=$(timeout 120s $CLICKHOUSE_CLIENT --query "$STMT" 2>&1) && RC=0 || RC=$?
        local ERROR=${OUTPUT//$'\n'/ }

        if [[ "$RC" -eq 0 ]]
        then
            return 0
        fi

        if [[ "$RC" -eq 124 ]]
        then
            echo "TIMEOUT: $STMT"
            exit 1
        fi

        if [[ -z "${ERROR}" ]]
        then
            echo "CLIENT EXIT $RC WITH EMPTY STDERR: $STMT"
            exit 1
        fi

        # Documented-retryable: too many concurrent metadata ALTERs. The server explicitly
        # tells us to retry, so we re-run the same statement until it succeeds.
        if [[ "${ERROR}" =~ "You can retry this error" \
            || "${ERROR}" =~ "Please retry this query" ]]
        then
            continue
        fi

        # Transient ZooKeeper-session state: the single replica briefly went read-only.
        # Retry until the session recovers (bounded by this thread's time budget).
        if [[ "${ERROR}" =~ "in readonly mode" ]]
        then
            continue
        fi

        # Benign teardown races: a pending ALTER/mutation could not be awaited because the
        # table or replica was shut down (e.g. a concurrent DROP at teardown or a server
        # restart triggered by another parallel test). Nothing left to do; stop and ignore.
        if [[ "${ERROR}" =~ "table shutdown was called" \
            || "${ERROR}" =~ "was shut down" ]]
        then
            return 0
        fi

        # Anything else is a genuine error this test exists to catch
        # (NOT_FOUND_COLUMN_IN_BLOCK, DUPLICATE_COLUMN, ...). Print it so the test fails:
        # the reference file is empty, so any output is a failure.
        echo "${ERROR}"
        return 0
    done
}

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        run_with_retry "ALTER TABLE alter_table ADD COLUMN $1 String DEFAULT '0'"
        run_with_retry "ALTER TABLE alter_table MODIFY COLUMN $1 UInt64"
        run_with_retry "ALTER TABLE alter_table DROP COLUMN $1"
    done
}

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        run_with_retry "INSERT INTO alter_table (a, b, c, d, e, f, g) SELECT rand(1), rand(2), rand(3), rand(4), rand(5), rand(6), rand(7) FROM numbers(1000)"
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
