#!/usr/bin/env bash
# `system.user_processes` must show up-to-date `MemoryCredits` for a running query that allocated memory
# and then holds it idle (without any allocation or free): the per-user counters are snapshotted only
# after the held interval of the running queries of the user has been charged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="test_user_05257_$(random_str 10)"
QUERY_ID="05257_${CLICKHOUSE_DATABASE}_$(random_str 10)"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $USER"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $USER"

# Build an IN-set of at least 32 MiB (2000 strings of 16 KiB) and hold it while sleeping 3 x 3 seconds.
# `max_block_size = 1` makes `sleepEachRow` run once per single-row block, within the 3-second cap.
# The query is sent over HTTP and with `query_metric_log_interval = 0`: the native client requests profile
# events from the server periodically and `system.query_metric_log` snapshots the query periodically, and
# both also charge the held interval, so a stale per-user snapshot would not be observable with them.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${USER}&query_id=${QUERY_ID}&max_threads=1&max_block_size=1&query_metric_log_interval=0" -d "
    SELECT number FROM system.numbers
    WHERE sleepEachRow(3) = 0
      AND concat(repeat('x', 16384), toString(number)) IN (SELECT concat(repeat('x', 16384), toString(number)) FROM system.numbers LIMIT 2000)
    LIMIT 3
    FORMAT Null" &

# Wait until the set is built and held.
for _ in {1..600}; do
    held=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.user_processes WHERE user = '$USER' AND memory_usage > 16 * 1048576")
    [ "$held" = "1" ] && break
    sleep 0.1
done

credits_before=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['MemoryCredits'] FROM system.user_processes WHERE user = '$USER'")
sleep 1.5
credits_after=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['MemoryCredits'] FROM system.user_processes WHERE user = '$USER'")

wait

$CLICKHOUSE_CLIENT -q "SELECT $credits_after > $credits_before"

$CLICKHOUSE_CLIENT -q "DROP USER $USER"
