#!/usr/bin/env bash
# Tags: long
#
# A `KILL`ed `SELECT` from the `primes` table function must abort promptly. A large `step` makes
# `PrimesSource::generate` spin in its inner loop calling `next` astronomically many times without
# yielding to the pipeline executor; without an in-loop cancellation check the query keeps running
# after `KILL QUERY` and the bounded `KILL QUERY` below trips its timeout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="primes_cancel_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04410_primes_err.txt"

# Huge step: after emitting the first prime, `generate` must skip ~1e11 primes before the next one,
# so a single `generate` call never returns. `max_rows_to_read = 0` disables the up-front row-estimate
# check so the query actually runs (and spins) instead of failing fast, regardless of randomized
# settings the harness may inject.
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    -q "SELECT * FROM primes(0, 1000000, 100000000000) SETTINGS max_rows_to_read = 0 FORMAT Null" >/dev/null 2>"$err" &
select_pid=$!

# Wait until the query is registered and running.
running=0
for _ in $(seq 1 600); do
    running=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
    [ "$running" = "1" ] && break
    sleep 0.1
done

if [ "$running" != "1" ]; then
    echo "query did not start"
    cat "$err"
# On the fixed server the cancel is observed inside the generate loop and `KILL QUERY` returns
# quickly; bound it so a regression (cancel ignored while spinning) fails the test instead of hanging.
elif timeout 10 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
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
