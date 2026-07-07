#!/usr/bin/env bash
# Tags: long, no-random-settings
#
# A `KILL`ed `SELECT` from the `primes` table function must abort promptly. A large `step` makes
# `PrimesSource::generate` spin in its inner loop calling `next` astronomically many times without
# yielding to the pipeline executor; without an in-loop cancellation check the query keeps running
# after `KILL QUERY` and the bounded `KILL QUERY` below trips its timeout.
#
# no-random-settings: the SELECT must keep running until this test KILLs it, and we measure cancellation
# latency ourselves. A randomized `max_execution_time` would terminate it early (or instead of our
# `KILL`), and a randomized `max_rows_to_read` would fail it up front; we need those limits at their
# (unlimited) defaults.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="primes_cancel_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04410_primes_err.txt"

# Huge step: after emitting the first prime, `generate` must skip ~1e11 primes before the next one,
# so a single `generate` call never returns. `max_rows_to_read = 0` disables the up-front row-estimate
# check so the query actually runs (and spins) instead of failing fast.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    -q "SELECT * FROM primes(0, 1000000, 100000000000) SETTINGS max_rows_to_read = 0 FORMAT Null" >/dev/null 2>"$err" &
select_pid=$!

# Wait until the query has actually been executing for a bounded interval, so the PipelineExecutor is
# attached and `PrimesSource::generate` is spinning in its loop. Gating only on the query being visible
# in `system.processes` is not enough: `ProcessList::insert` makes it visible before the executor is
# attached, and if `KILL QUERY` won that race the cancellation would be thrown from
# `addPipelineExecutor` rather than the in-loop check, so the test could pass even without the fix.
elapsed=0
for _ in $(seq 1 600); do
    elapsed=$(${CLICKHOUSE_CLIENT} -q "SELECT max(elapsed) FROM system.processes WHERE query_id = '$query_id'")
    if [ -n "$elapsed" ] && awk "BEGIN{exit !($elapsed > 1)}"; then break; fi
    sleep 0.1
done

if [ -z "$elapsed" ] || ! awk "BEGIN{exit !($elapsed > 1)}" 2>/dev/null; then
    echo "query did not reach the generate loop"
    cat "$err"
# On the fixed server the cancel is observed inside the generate loop and `KILL QUERY` returns
# quickly; bound it so a regression (cancel ignored while spinning) fails the test instead of hanging.
# The bound only has to separate "returns" from "spins forever", so it is deliberately generous: the
# regression never returns (`generate` skips ~1e17 primes), while on a slow, heavily loaded sanitizer
# build the bounded `KILL` still has to pay a fresh `clickhouse-client` startup plus the cancel + the
# 100ms-granularity sync wait. A tight bound (10s) flaked on `amd_msan` under load, where a single
# client invocation alone can take ~8s; 60s keeps the regression caught while removing the flake.
elif timeout 60 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
then
    echo "killed promptly"
else
    echo "KILL QUERY SYNC did not return in time"
fi

# Terminate the background client so the test always finishes bounded: on the fixed path it has
# already exited (cancelled); on a regression it is still attached to the spinning query.
kill "$select_pid" 2>/dev/null
wait "$select_pid" 2>/dev/null

rm -f "$err"
