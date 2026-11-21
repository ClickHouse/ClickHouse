#!/usr/bin/env bash
# Tags: race, zookeeper, no-flaky-check, no-replicated-database, no-random-settings, no-random-merge-tree-settings
# no-flaky-check: This test stresses concurrent ALTER and INSERT to guard against a
# column-resolution regression. It is expected to pass on master; it should fail only
# if that regression returns (a genuine bug surfaces as `NOT_FOUND_COLUMN_IN_BLOCK`,
# `DUPLICATE_COLUMN`, etc.). Re-running it many times under sanitizers and the thread
# fuzzer exceeds the per-test budget, which is why the flaky check is disabled.
#
# no-random-settings, no-random-merge-tree-settings: this is a timing-sensitive concurrency
# test, not a coverage test for query or merge-tree settings. Its contract (each side of the
# race must make progress within the time budget, otherwise the test exercised nothing) is
# orthogonal to those settings, while randomized merge-tree settings (huge merge/granularity
# thresholds) only slow down the data-rewriting `MODIFY COLUMN` mutation enough to blow the
# budget. CI's randomized-settings diagnosis confirmed this: the test passes 3/3 without
# randomization and fails 3/3 with the randomized settings on a slow (msan) build. Pinning the
# settings removes that timing variance without weakening what the test actually checks.
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
# To make sure the test cannot pass without ever exercising the concurrent ALTER/INSERT it
# promises, each worker records its progress: every `thread_alter` worker records how many
# full ADD -> MODIFY -> DROP cycles it completed, and every `thread_insert` worker records how
# many INSERTs landed. The script fails if no worker completed a single ALTER cycle, or if no
# INSERT landed at all (e.g. if every attempt only ever hit a retryable error until the time
# budget ran out) - either way the promised race was never actually exercised. The one
# exception is a run disrupted by a benign table/replica shutdown (a concurrent restart): that
# is not the test's fault, so it records a "disrupted" marker and the progress requirement is
# waived.
#
# Because a single ADD -> MODIFY -> DROP cycle can take longer than the soft time budget on the
# slowest builds (sanitizers + s3 storage + parallel load), the workers do not give up at the
# soft budget when a side has made no progress yet: they keep running - INSERTs and ALTERs
# still concurrent - until both sides have made progress or a generous hard cap is reached (see
# `within_budget`). Only then, if a side still made zero progress, does the test fail. This
# keeps the guard meaningful without flaking on slow builds, where the right answer is to run a
# little longer, not to declare the race never happened.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Per-worker progress files: `.cycles.<col>` for each `thread_alter` worker and
# `.inserts.<id>` for each `thread_insert` worker. See the progress accounting at the end of
# the script. The prefix is unique per test invocation so parallel runs of the stateless
# suite do not collide.
PROGRESS_PREFIX="${CLICKHOUSE_TMP}/03518_progress_${CLICKHOUSE_TEST_UNIQUE_NAME}"
# Markers a worker touches the first time each side of the race makes progress: a completed
# ALTER cycle and a landed INSERT. `within_budget` consults them to decide when the workers may
# stop (see below). The single `rm -f "${PROGRESS_PREFIX}"*` glob clears every per-invocation
# file at once: the `.cycles.*`/`.inserts.*` counters, the progress markers, and the
# `_disrupted` marker.
ALTER_PROGRESS_MARKER="${PROGRESS_PREFIX}_alter_progress"
INSERT_PROGRESS_MARKER="${PROGRESS_PREFIX}_insert_progress"
rm -f "${PROGRESS_PREFIX}"*

trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"; rm -f "${PROGRESS_PREFIX}"*' EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
# Disable the throttles that defer `MODIFY COLUMN`'s data mutation behind merges: under this test's
# concurrent INSERT load they can starve the first mutation, which (replicated alters finish in strict
# version order) stalls every later `MODIFY` until `timeout` fires with a spurious failure. CI report:
# https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=2a3a502d2ed0af65bb0a5c91223c91c1b2e28047&name_0=MasterCI&name_1=Stateless%20tests%20%28arm_binary%2C%20parallel%29
$CLICKHOUSE_CLIENT << SQL
CREATE TABLE alter_table (a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03518/alter_table', 'r1')
ORDER BY a
PARTITION BY b % 10
SETTINGS old_parts_lifetime = 1,
  max_replicated_mutations_in_queue = 100000,
  number_of_free_entries_in_pool_to_execute_mutation = 0
SQL

# True while the workers should keep issuing (and retrying) statements. Every worker - both
# `thread_alter` and `thread_insert` loops, and the retry loop inside `run_with_retry` - gates
# on this, so they all wind down together.
#
# The contract: always run through the soft budget (`SOFT_DEADLINE`); past it, keep going only
# until BOTH sides have made progress - an ALTER cycle completed and an INSERT landed, recorded
# via the progress markers. That is what lets a slow build which needs more than the soft
# budget to land its first ALTER cycle finish it instead of being failed by the progress guard,
# while INSERTs keep running concurrently the whole time so the race is still genuinely
# exercised. `HARD_DEADLINE` is a backstop so a truly stuck run still terminates well within the
# 600s per-test budget - and is then correctly reported as NO ALTER/INSERT PROGRESS.
function within_budget()
{
    if [ "$SECONDS" -ge "$HARD_DEADLINE" ]
    then
        return 1
    fi
    if [ "$SECONDS" -ge "$SOFT_DEADLINE" ] \
        && [ -e "$ALTER_PROGRESS_MARKER" ] \
        && [ -e "$INSERT_PROGRESS_MARKER" ]
    then
        return 1
    fi
    return 0
}

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

    while within_budget
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

        # Benign teardown races: a pending ALTER/mutation/insert could not complete because
        # the table or replica was shut down (e.g. a concurrent DROP at teardown or a server
        # restart triggered by another parallel test). This includes the read-only variant
        # `Table is in readonly mode due to shutdown: ...` raised by `ReplicatedMergeTreeSink`
        # on the INSERT path, which must be classified here - before the generic read-only
        # retry below - so a permanent shutdown is not mistaken for a transient session blip
        # and retried until the time budget runs out. Nothing left to do for this worker; stop
        # without printing (non-zero so the caller breaks its dependent sequence), and record
        # that the run was disrupted so the progress requirement below is waived.
        if [[ "${ERROR}" =~ "table shutdown was called" \
            || "${ERROR}" =~ "was shut down" \
            || "${ERROR}" =~ "due to shutdown" ]]
        then
            touch "${PROGRESS_PREFIX}_disrupted"
            return 1
        fi

        # Transient ZooKeeper-session state: the single replica briefly went read-only (e.g.
        # `Table is in readonly mode (replica path: ...)`). This is not a shutdown - the
        # session recovers - so retry until it does (bounded by this thread's time budget).
        if [[ "${ERROR}" =~ "in readonly mode" ]]
        then
            continue
        fi

        # Anything else is a genuine error this test exists to catch
        # (NOT_FOUND_COLUMN_IN_BLOCK, DUPLICATE_COLUMN, ...). Print it so the test fails:
        # the reference file is empty, so any output is a failure.
        echo "${ERROR}"
        return 0
    done

    # `within_budget` became false (the run is winding down, or the hard cap was hit) while the
    # statement had not yet landed - only retryable errors were seen. Signal the caller so it
    # does not run the next, dependent step.
    return 1
}

function thread_alter()
{
    local col="$1"
    local cycles=0
    while within_budget
    do
        run_with_retry "ALTER TABLE alter_table ADD COLUMN $col String DEFAULT '0'" || break
        run_with_retry "ALTER TABLE alter_table MODIFY COLUMN $col UInt64" || break
        run_with_retry "ALTER TABLE alter_table DROP COLUMN $col" || break
        cycles=$((cycles + 1))
        # Record that the ALTER side has made progress, so `within_budget` may let the run wind
        # down once the INSERT side has too. Idempotent: `touch` from either ALTER worker is fine.
        touch "$ALTER_PROGRESS_MARKER"
    done
    # Record completed cycles so the parent can fail the test if no worker made progress.
    echo "$cycles" > "${PROGRESS_PREFIX}.cycles.${col}"
}

function thread_insert()
{
    local id="$1"
    local inserts=0
    while within_budget
    do
        # A non-zero return means `run_with_retry` gave up before the INSERT landed: either
        # the time budget ran out or the table/replica was shut down (disrupted). Stop the
        # worker cleanly instead of letting `set -e` fail the whole test for that benign
        # shutdown/restart race. A genuine INSERT error is printed by `run_with_retry` (which
        # then returns 0), so it still fails the empty-reference test.
        run_with_retry "INSERT INTO alter_table (a, b, c, d, e, f, g) SELECT rand(1), rand(2), rand(3), rand(4), rand(5), rand(6), rand(7) FROM numbers(1000)" || break
        inserts=$((inserts + 1))
        # Record that the INSERT side has made progress (see `within_budget`).
        touch "$INSERT_PROGRESS_MARKER"
    done
    # Record landed INSERTs so the parent can fail the test if no INSERT ever ran concurrently
    # with the ALTERs (otherwise the test would prove only the ALTER side of the race).
    echo "$inserts" > "${PROGRESS_PREFIX}.inserts.${id}"
}

# Soft budget: in the common case both sides make progress within a few seconds, so the workers
# stop at the soft deadline. Hard cap: when a side has not made a single bit of progress yet -
# which happens on the slowest sanitizer + s3 storage + parallel builds, where one
# ADD -> MODIFY -> DROP cycle alone can exceed the soft budget - the workers keep running past
# it, with INSERTs and ALTERs still concurrent, until the first progress lands or the hard cap
# is reached (see `within_budget`). The hard cap stays well under the 600s per-test budget.
# `$SECONDS` is the seconds-since-shell-start timeline shared by the background workers, so the
# deadlines are computed once here and read by every worker.
TIMEOUT=10
MAX_TIMEOUT=180
SOFT_DEADLINE=$((SECONDS + TIMEOUT))
HARD_DEADLINE=$((SECONDS + MAX_TIMEOUT))

pids=()
thread_alter h & pids+=("$!")
thread_insert 1 & pids+=("$!")
thread_alter i & pids+=("$!")
thread_insert 2 & pids+=("$!")

rc=0
for pid in "${pids[@]}"
do
    wait "$pid" || rc=1
done

# A run that only ever hit retryable errors until its budget ran out completes zero full
# ALTER cycles or lands zero INSERTs, in which case the empty-reference test would otherwise
# pass without having exercised the concurrent ALTER/INSERT it promises. Require at least one
# completed ALTER cycle and at least one landed INSERT across the workers, and fail explicitly
# otherwise - unless the run was disrupted by a benign table/replica shutdown, which is not
# the test's fault.
total_cycles=0
for f in "${PROGRESS_PREFIX}".cycles.*
do
    [ -e "$f" ] || continue
    total_cycles=$((total_cycles + $(cat "$f")))
done

total_inserts=0
for f in "${PROGRESS_PREFIX}".inserts.*
do
    [ -e "$f" ] || continue
    total_inserts=$((total_inserts + $(cat "$f")))
done

if [[ ! -e "${PROGRESS_PREFIX}_disrupted" ]]
then
    if [[ "$total_cycles" -eq 0 ]]
    then
        echo "NO ALTER PROGRESS: completed zero full ALTER ADD/MODIFY/DROP cycles - the test did not exercise concurrent ALTERs"
        rc=1
    fi
    if [[ "$total_inserts" -eq 0 ]]
    then
        echo "NO INSERT PROGRESS: zero INSERTs landed - the test did not exercise concurrent INSERTs against the ALTERing table"
        rc=1
    fi
fi

exit "$rc"
