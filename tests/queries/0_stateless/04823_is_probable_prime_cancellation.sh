#!/usr/bin/env bash
# Tags: long, no-fasttest, no-coverage, no-flaky-check
# no-coverage: per-test coverage instrumentation stretches the fixed side of the timed cases.
# no-flaky-check: The test verifies a timeout-based behavior and is not suitable for rerun-based flakiness detection.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every timing case bounds how long an `isProbablePrime` call runs past the deadline it was given: the fix
# bounds it, the unfixed code does not. Each one keeps its prologue - reading rows, building a block - small
# and puts the cost inside the function, because the prologue is not interruptible by this fix. A sanitizer
# build stretches both sides, so the bound scales; the `no-coverage` tag above skips coverage builds, whose
# instrumentation stretches the fixed side past the deadline instead of merely slowing both sides.
DEADLINE_MS=2000
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
BOUND=$((SCALE * 8000))
# What a bound allows on top of its deadline. A case that needs a deadline of its own keeps this
# allowance rather than the bound itself, so shortening a deadline does not loosen the assertion with it.
SLACK_MS=$((BOUND - DEADLINE_MS))
# `max_execution_time` is given in seconds; every deadline here is a whole number of milliseconds.
to_seconds() { printf '%d.%03d' "$(($1 / 1000))" "$(($1 % 1000))"; }

# The runner randomizes parallel replicas into every client call, and a `system` table read under it wants
# a cluster a single-node server may not have.
LOG_PINS="--enable_parallel_replicas 0"

# A 2^255-19 prime forces the full Miller-Rabin rounds; the even sibling and a value that fits in UInt64
# take the cheap exits instead. `materialize` is what makes the expression run per row: without it the call
# is constant-folded once and the whole loop disappears, which would make every timing case vacuous.
P256=57896044618658097711785492504343953926634992332820282019728792003956564819949
P128=170141183460469231731687303715884105727

# $1 = label, $2 = query, $3 = overflow mode (default "throw"), $4 = the deadline in milliseconds
# (default `DEADLINE_MS`)
#
# Two messages report the same enforced deadline and only one carries a number: `CancellationChecker` can
# win the race against this function's own check, and its error then has no elapsed part to read. Accepting
# that on wall time would assert query-level cancellation instead of the in-function checkpoint, so it is
# reported as inconclusive rather than passed.
run() {
    local label="$1" query="$2" mode="${3:-throw}" deadline_ms="${4:-$DEADLINE_MS}"
    local query_id="04823_${label// /_}_${CLICKHOUSE_DATABASE}" output elapsed_ms rows breaks
    local bound=$((deadline_ms + SLACK_MS))
    # shellcheck disable=SC2086
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        --max_execution_time "$(to_seconds "$deadline_ms")" --timeout_overflow_mode "$mode" \
        --log_profile_events 1 --max_threads 1 \
        --query "$query" 2>&1)

    if [ "$mode" = break ]; then
        # Under `break` the pipeline turns the throw into a graceful finish, so the client sees no error and
        # no elapsed time to read. The cause is `ProfileEvents['OverflowBreak']`, and the row count is read
        # too: `max` over no rows returns 0, which would otherwise be below the bound and score as a pass.
        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        # shellcheck disable=SC2086
        read -r rows elapsed_ms breaks < <(${CLICKHOUSE_CLIENT} $LOG_PINS --query "
            SELECT count(), max(query_duration_ms), max(ProfileEvents['OverflowBreak']) FROM system.query_log
            WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart'")
        if [ "${rows:-0}" = 0 ]; then
            echo "$label: NO QUERY LOG ROW"
        elif [ "${breaks:-0}" = 0 ]; then
            # A fast failure that never reached the deadline, e.g. a read limit or an out-of-memory.
            echo "$label: DEADLINE NEVER OBSERVED (no OverflowBreak)"
        elif [ "$elapsed_ms" -lt "$bound" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
        return
    fi

    elapsed_ms=$(printf '%s' "$output" | grep -oP 'elapsed \K[0-9]+(?=\.)' | head -1)
    # A verdict rather than the number itself keeps the reference stable across machine speeds.
    if [ -n "$elapsed_ms" ]; then
        if [ "$elapsed_ms" -lt "$bound" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label: NOT STOPPED BY THE DEADLINE: $(printf '%s' "$output" | grep -oE 'Code: [0-9]+' | head -1)"
    else
        echo "$label: cancelled before the pipeline started"
    fi
}

# 1. The shape the CI report showed: one block of UInt256 rows each running the full rounds. Unfixed this
#    reports about 200000 ms elapsed against a 2000 ms deadline.
run "uint256 rounds" \
    "SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 64)) FROM numbers(10000) SETTINGS max_block_size = 10000"

# 2. The "break" overflow mode, where checkTimeLimit() returns false instead of throwing, so a path that
#    never calls it ignores the deadline entirely. The client sees no error here, so the verdict reads the
#    duration and the break counter out of the query log rather than the client output.
run "uint256 break" \
    "SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 64)) FROM numbers(10000) SETTINGS max_block_size = 10000 FORMAT Null" \
    break

# 3. The UInt128 carrier, which reaches the same rounds through its own type instantiation and costs about a
#    fifth as much per row, confirming the checkpoint is not tied to one width.
run "uint128 rounds" \
    "SELECT sum(isProbablePrime(toUInt128(materialize('$P128')), 64)) FROM numbers(100000) SETTINGS max_block_size = 100000"

# 4. Rows that never reach the rounds, which the callback alone would never bound: a wide value that fits in
#    UInt64 returns through the first cheap exit, so those rows are charged through the budget instead of
#    checking each one. The row count is what makes a single block outlast the deadline at that price, and it
#    stays under the 20M `max_rows_to_read` of the CI `default` profile (tests/config/users.d/limits.yaml) so
#    the block is reached at all. `materialize` wraps the finished UInt256 rather than a narrower literal, so
#    the per-row prologue is a copy rather than a conversion: 368 ms of the 16M rows here, against 57000 ms
#    for the block itself. This case carries its own scaled deadline because its prologue is the fixed side
#    and a sanitizer stretches that too, while `run` keeps the allowance on top of it unchanged.
run "cheap exit budget" \
    "SELECT sum(isProbablePrime(materialize(toUInt256(18446744073709551557)), 256)) FROM numbers(16000000) SETTINGS max_block_size = 16000000" \
    throw $((SCALE * DEADLINE_MS))

# 5. KILL QUERY, a different channel from the elapsed-time limit: it sets the killed flag and surfaces
#    through a separate branch of the same check. No time limit is set, so only the kill can stop the query,
#    and what is asserted is how long the synchronous kill waits. `KILL ... SYNC` polls until the target
#    leaves the process list, so an unfixed server holds it for the rest of the block and then still
#    reports a number rather than hanging.
#    The row count is sized against the FASTEST build rather than this one, because the bound scales with
#    the sanitizer while this work does not: an instrumented build is optimized where a debug build is
#    not, so a row costs 42 ms there against 82 ms here while the bound doubles. 2000 rows keeps an
#    unfixed block at 5x the bound on the tighter of the two. The fixed side is independent of the count,
#    since it notices the flag within one row.
KILL_BOUND=$((SCALE * 8000))
kill_id="04823_kill_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$kill_id" --max_threads 1 \
    --query "SELECT sum(isProbablePrime(materialize(toUInt256('$P256')), 256)) FROM numbers(2000) SETTINGS max_block_size = 2000" \
    > /dev/null 2>&1 &
kill_bg=$!

# Waiting for the query to be RUNNING rather than merely visible, because two windows serve a kill through
# the generic path and would return promptly even unfixed: `ProcessList` publishes the query before the
# executor is attached, and `ExpressionActions` checks cancellation after every action, so a conversion
# done per row in the prologue is interruptible on its own. `materialize` wraps the finished `UInt256`
# rather than the string, which keeps that prologue a copy rather than one parse per row.
kill_seen=0
for _ in $(seq 1 200); do
    # shellcheck disable=SC2086
    if [ "$(${CLICKHOUSE_CLIENT} $LOG_PINS --query "SELECT max(elapsed) > 0.5 FROM system.processes WHERE query_id = '$kill_id'")" = "1" ]; then
        kill_seen=1
        break
    fi
    sleep 0.05
done

# `KILL ... SYNC` polls without a deadline of its own, so it is given a query id and a hard timeout: a
# regression must fail with a readable verdict instead of hanging. The runner arms its own alarm at
# `int(timeout * 1.1) + 60`, 720 s by default, so a guard above that would never be the one to fire.
sync_id="${kill_id}_sync"
# shellcheck disable=SC2086
timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$sync_id" --query "KILL QUERY WHERE query_id = '$kill_id' SYNC" > /dev/null 2>&1
wait $kill_bg 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

if [ "$kill_seen" != 1 ]; then
    echo "kill query: TARGET NEVER REACHED THE ROUNDS"
else
    # A kill returning in milliseconds never waited for a running call, so it measures the race and not the
    # checkpoint. Reported as inconclusive rather than as a pass.
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $LOG_PINS --query "
        SELECT multiIf(count() = 0, 'kill query: NO QUERY LOG ROW',
                       max(query_duration_ms) < 10, 'kill query: TARGET WAS NOT RUNNING WHEN KILLED',
                       max(query_duration_ms) < $KILL_BOUND, 'kill query: killed within bound',
                       'kill query: BLOCKED ' || toString(max(query_duration_ms)) || 'ms')
        FROM system.query_log
        WHERE query_id = '$sync_id' AND current_database = currentDatabase() AND type != 'QueryStart'"
fi

# 6. Liveness control for the paths the fix deliberately does not touch. With no deadline set, the cheap
#    exits and the narrow path must still return the documented answers over a full block, so a "fix" that
#    threw unconditionally, or a throttle that skipped rows, fails here. Their cost is a performance
#    property, not a correctness one, and a wall-clock ceiling wide enough not to be flaky in CI cannot
#    redden on the per-row-check regression it would be guarding, so the numbers live in the PR body.
${CLICKHOUSE_CLIENT} --max_threads 1 --query \
    "SELECT sum(isProbablePrime((bitShiftLeft(toUInt256(1), 200) + toUInt256(number)) * 2, 256)) FROM numbers(1000000) SETTINGS max_block_size = 1000000"
${CLICKHOUSE_CLIENT} --max_threads 1 --query \
    "SELECT sum(isProbablePrime(materialize(18446744073709551557))) FROM numbers(1000000) SETTINGS max_block_size = 1000000"

# 7. Results are unchanged by the checkpoint, across both cheap exits, the rounds themselves, and the
#    documented examples. The checkpoint sits between the exits and the rounds, so this is where a
#    misplacement would show up as a changed answer rather than as a timing difference.
${CLICKHOUSE_CLIENT} --query "
    SELECT isProbablePrime(17), isProbablePrime(18), isProbablePrime(18446744073709551557),
           isProbablePrime(toUInt128('$P128')), isProbablePrime(toUInt256('$P256')),
           isProbablePrime(toUInt256('$P256'), 5), isProbablePrime(toUInt256('$P256'), 256),
           isProbablePrime(toUInt128(0)), isProbablePrime(toUInt256(1)), isProbablePrime(toUInt256(2)),
           isProbablePrime(toUInt128(65537)), isProbablePrime(toUInt256(18446744073709551557)),
           isProbablePrime(toUInt256(toUInt256('$P256') - toUInt256(2)))"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 5)),
           sum(isProbablePrime(toUInt128(materialize('$P128')), 5)) FROM numbers(10)"

# 8. `rounds` validation is unchanged: both ends of the accepted range still reject.
${CLICKHOUSE_CLIENT} --query "SELECT isProbablePrime(toUInt256('$P256'), 0)" 2>&1 | grep -o -m1 BAD_ARGUMENTS
${CLICKHOUSE_CLIENT} --query "SELECT isProbablePrime(toUInt256('$P256'), 257)" 2>&1 | grep -o -m1 BAD_ARGUMENTS
