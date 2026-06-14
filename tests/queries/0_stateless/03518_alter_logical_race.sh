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
#
# To make sure the test cannot pass without ever exercising a concurrent ALTER, each
# `thread_alter` worker records how many full ADD -> MODIFY -> DROP cycles it completed,
# and the script fails if neither worker completed a single cycle (e.g. if every attempt
# only ever hit a retryable error until the time budget ran out). The one exception is a
# run disrupted by a benign table/replica shutdown (a concurrent restart): that is not the
# test's fault, so it records a "disrupted" marker and the progress requirement is waived.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Per-worker progress files (one per `thread_alter` column). See the cycle accounting at
# the end of the script. The prefix is unique per test invocation so parallel runs of the
# stateless suite do not collide.
PROGRESS_PREFIX="${CLICKHOUSE_TMP}/03518_progress_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "${PROGRESS_PREFIX}".*

trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"; rm -f "${PROGRESS_PREFIX}".* "${PROGRESS_PREFIX}_disrupted"' EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03518/alter_table', 'r1') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

# Run a single statement, retrying it on transient/retryable conditions until it
# actually lands (or until the time budget for this thread runs out). Retrying is what
# keeps each thread's ADD -> MODIFY -> DROP sequence consistent, so that any
# NOT_FOUND_COLUMN_IN_BLOCK / DUPLICATE_COLUMN that does appear is a genuine regression.
#
# Returns 0 once the statement has actually landed (and also after a genuine error, which
# it prints first so the empty-reference test fails). Returns non-zero - without printing -
# when it had to give up before the statement landed: either the thread's time budget was
# exhausted while only retryable errors were seen, or the table/replica was shut down. The
# caller uses that to stop its ADD -> MODIFY -> DROP sequence instead of issuing the next,
# dependent step against a column state the previous step never actually established.
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
        # restart triggered by another parallel test). Nothing left to do for this worker;
        # stop without printing (non-zero so the caller breaks its dependent sequence), and
        # record that the run was disrupted so the progress requirement below is waived.
        if [[ "${ERROR}" =~ "table shutdown was called" \
            || "${ERROR}" =~ "was shut down" ]]
        then
            touch "${PROGRESS_PREFIX}_disrupted"
            return 1
        fi

        # Anything else is a genuine error this test exists to catch
        # (NOT_FOUND_COLUMN_IN_BLOCK, DUPLICATE_COLUMN, ...). Print it so the test fails:
        # the reference file is empty, so any output is a failure.
        echo "${ERROR}"
        return 0
    done

    # Time budget exhausted while the statement had not yet landed (only retryable errors
    # were seen). Signal the caller so it does not run the next, dependent step.
    return 1
}

function thread_alter()
{
    local col="$1"
    local cycles=0
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        run_with_retry "ALTER TABLE alter_table ADD COLUMN $col String DEFAULT '0'" || break
        run_with_retry "ALTER TABLE alter_table MODIFY COLUMN $col UInt64" || break
        run_with_retry "ALTER TABLE alter_table DROP COLUMN $col" || break
        cycles=$((cycles + 1))
    done
    # Record completed cycles so the parent can fail the test if no worker made progress.
    echo "$cycles" > "${PROGRESS_PREFIX}.${col}"
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

# A run that only ever hit retryable errors until its budget ran out completes zero full
# ALTER cycles, in which case the empty-reference test would otherwise pass without having
# exercised a single concurrent ALTER. Require at least one completed cycle across the
# workers, and fail explicitly otherwise - unless the run was disrupted by a benign
# table/replica shutdown, which is not the test's fault.
total_cycles=0
for f in "${PROGRESS_PREFIX}".*
do
    [ -e "$f" ] || continue
    total_cycles=$((total_cycles + $(cat "$f")))
done

if [[ "$total_cycles" -eq 0 && ! -e "${PROGRESS_PREFIX}_disrupted" ]]
then
    echo "NO PROGRESS: completed zero full ALTER ADD/MODIFY/DROP cycles - the test did not exercise concurrent ALTERs"
    rc=1
fi

exit "$rc"
