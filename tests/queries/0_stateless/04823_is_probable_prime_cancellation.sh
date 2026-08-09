#!/usr/bin/env bash
# Tags: long, no-fasttest, no-flaky-check
# no-flaky-check: The test verifies a timeout-based behavior and is not suitable for rerun-based flakiness detection.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every timing case bounds how long an `isProbablePrime` call runs past the deadline it was given: the fix
# bounds it, the unfixed code does not. Each one keeps its prologue - reading rows, building a block - small
# and puts the cost inside the function, because the prologue is not interruptible by this fix. A sanitizer
# or coverage build stretches both sides, so the bound scales; coverage never reaches CXX_FLAGS and has to be
# read from its own `system.build_options` row.
DEADLINE_MS=2000
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
BOUND=$((SCALE * 8000))
# What a bound allows on top of its deadline. A case that needs a deadline of its own keeps this
# allowance rather than the bound itself, so shortening a deadline does not loosen the assertion with it.
SLACK_MS=$((BOUND - DEADLINE_MS))
# `max_execution_time` is given in seconds; every deadline here is a whole number of milliseconds.
to_seconds() { printf '%d.%03d' "$(($1 / 1000))" "$(($1 % 1000))"; }
DEADLINE=$(to_seconds "$DEADLINE_MS")

# A 2^255-19 prime forces the full Miller-Rabin rounds; the even sibling and a value that fits in UInt64
# take the cheap exits instead. `materialize` is what makes the expression run per row: without it the call
# is constant-folded once and the whole loop disappears, which would make every timing case vacuous.
P256=57896044618658097711785492504343953926634992332820282019728792003956564819949
P128=170141183460469231731687303715884105727

# $1 = label, $2 = query, $3 = overflow mode (default "throw"), $4 = query id (default none),
# $5 = the deadline in milliseconds (default `DEADLINE_MS`)
#
# Two messages report the same enforced deadline and only one carries a number: `CancellationChecker` can
# win the race against this function's own check, and its error then has no elapsed part to read. Accepting
# that on wall time would assert query-level cancellation instead of the in-function checkpoint, so it is
# reported as inconclusive rather than passed.
run() {
    local label="$1" query="$2" mode="${3:-throw}" query_id="${4:-}" deadline_ms="${5:-$DEADLINE_MS}"
    local output elapsed_ms
    local bound=$((deadline_ms + SLACK_MS))
    # shellcheck disable=SC2086
    output=$(timeout 900 ${CLICKHOUSE_CLIENT} --max_execution_time "$(to_seconds "$deadline_ms")" --timeout_overflow_mode "$mode" \
        --max_threads 1 \
        ${query_id:+--query_id "$query_id"} \
        --query "$query" 2>&1)
    elapsed_ms=$(printf '%s' "$output" | grep -oP 'elapsed \K[0-9]+(?=\.)' | head -1)
    # A verdict rather than the number itself keeps the reference stable across machine speeds.
    if [ -n "$elapsed_ms" ]; then
        if [ "$elapsed_ms" -lt "$bound" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label: no timeout"
    else
        echo "$label: cancelled before the pipeline started"
    fi
}

# 1. The shape the CI report showed: one block of UInt256 rows each running the full rounds. Unfixed this
#    reports about 200000 ms elapsed against a 1000 ms deadline.
run "uint256 rounds" \
    "SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 64)) FROM numbers(10000) SETTINGS max_block_size = 10000"

# 2. The "break" overflow mode, where checkTimeLimit() returns false instead of throwing, so a path that
#    never calls it ignores the deadline entirely. What is asserted is the wall time, not the presence of an
#    error: this mode ends the query by cutting it short rather than by reporting, so the deadline is observed
#    as a bounded run. Unfixed this takes about 200000 ms against the same deadline.
break_start=$(date +%s%N)
timeout 900 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode break --max_threads 1 \
    --query "SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 64)) FROM numbers(10000) SETTINGS max_block_size = 10000 FORMAT Null" \
    > /dev/null 2>&1
break_ms=$(( ($(date +%s%N) - break_start) / 1000000 ))
if [ "$break_ms" -lt "$((BOUND * 2))" ]; then
    echo "uint256 break: stopped within bound"
else
    echo "uint256 break: OVERSHOT ${break_ms} ms"
fi

# 3. The UInt128 carrier, which reaches the same rounds through its own type instantiation and costs about a
#    fifth as much per row, confirming the checkpoint is not tied to one width.
run "uint128 rounds" \
    "SELECT sum(isProbablePrime(toUInt128(materialize('$P128')), 64)) FROM numbers(100000) SETTINGS max_block_size = 100000"

# 4. Rows that never reach the rounds, which the callback alone would never bound: a wide value that fits in
#    UInt64 returns through the first cheap exit at about 0.26 us, so those rows are charged through the
#    budget instead of checking each one. The row count is what makes a single block outlast the deadline at
#    that price; the arithmetic stays in UInt256 so no row wraps above the UInt64 range and reaches the
#    rounds by accident. The prologue for this shape is about 1650 ms of the bound, so the block itself is
#    what the remainder measures.
run "cheap exit budget" \
    "SELECT sum(isProbablePrime(toUInt256(toUInt256(18446744073709551557) - toUInt256(number)), 256)) FROM numbers(40000000) SETTINGS max_block_size = 40000000"

# 5. KILL QUERY, a different channel from the elapsed-time limit: it sets the killed flag and surfaces
#    through a separate branch of the same check. No time limit is set, so only the kill can stop the query,
#    and what is asserted is how long the synchronous kill waits. Both halves are needed - that the target
#    was really running, and that KILL reported killing it - otherwise a query that never started would be
#    "killed" instantly.
kill_id="${CLICKHOUSE_DATABASE}_is_probable_prime_kill"
${CLICKHOUSE_CLIENT} --query_id "$kill_id" --max_threads 1 \
    --query "SELECT sum(isProbablePrime(toUInt256(materialize('$P256')), 256)) FROM numbers(1000000) SETTINGS max_block_size = 1000000" \
    > /dev/null 2>&1 &
kill_bg=$!
kill_seen=0
for _ in $(seq 1 100); do
    if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$kill_id'")" = 1 ]; then
        kill_seen=1
        break
    fi
    sleep 0.1
done
kill_start=$(date +%s%N)
kill_rows=$(${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$kill_id' SYNC FORMAT TSV" 2>/dev/null | grep -c "$kill_id")
kill_ms=$(( ($(date +%s%N) - kill_start) / 1000000 ))
wait $kill_bg 2>/dev/null
if [ "$kill_seen" != 1 ]; then
    echo "kill query: TARGET NEVER RAN"
elif [ "$kill_rows" != 1 ]; then
    echo "kill query: KILL DID NOT REPORT THE TARGET"
elif [ "$kill_ms" -lt "$((BOUND * 2))" ]; then
    echo "kill query: killed within bound"
else
    echo "kill query: OVERSHOT ${kill_ms} ms"
fi

# 6. The cheap exits keep costing what they cost: they must not start paying for a cancellation check each.
#    An even 256-bit value returns through the second exit in well under a microsecond, so a per-row check
#    here would add a million of them and show up as a multiple of this ceiling. The prologue is kept out by
#    deriving the value arithmetically rather than parsing a string per row.
even_start=$(date +%s%N)
even_sum=$(${CLICKHOUSE_CLIENT} --max_threads 1 --query \
    "SELECT sum(isProbablePrime((bitShiftLeft(toUInt256(1), 200) + toUInt256(number)) * 2, 256)) FROM numbers(1000000) SETTINGS max_block_size = 1000000")
even_ms=$(( ($(date +%s%N) - even_start) / 1000000 ))
if [ "$even_sum" != 0 ]; then
    echo "cheap exit results: WRONG ($even_sum, expected 0)"
elif [ "$even_ms" -lt "$((SCALE * 20000))" ]; then
    echo "cheap exit results: correct and not slowed"
else
    echo "cheap exit results: SLOWED ${even_ms} ms"
fi

# 7. The narrow path is untouched by the fix and must stay that way: it is exact, bounded, and cannot hang,
#    so a check there would be a pure regression.
narrow_start=$(date +%s%N)
narrow_sum=$(${CLICKHOUSE_CLIENT} --max_threads 1 --query \
    "SELECT sum(isProbablePrime(materialize(18446744073709551557))) FROM numbers(1000000) SETTINGS max_block_size = 1000000")
narrow_ms=$(( ($(date +%s%N) - narrow_start) / 1000000 ))
if [ "$narrow_sum" != 1000000 ]; then
    echo "narrow path: WRONG ($narrow_sum, expected 1000000)"
elif [ "$narrow_ms" -lt "$((SCALE * 40000))" ]; then
    echo "narrow path: correct and not slowed"
else
    echo "narrow path: SLOWED ${narrow_ms} ms"
fi

# 8. Results are unchanged by the checkpoint, across both cheap exits, the rounds themselves, and the
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

# 9. `rounds` validation is unchanged: both ends of the accepted range still reject.
${CLICKHOUSE_CLIENT} --query "SELECT isProbablePrime(toUInt256('$P256'), 0)" 2>&1 | grep -o -m1 BAD_ARGUMENTS
${CLICKHOUSE_CLIENT} --query "SELECT isProbablePrime(toUInt256('$P256'), 257)" 2>&1 | grep -o -m1 BAD_ARGUMENTS
