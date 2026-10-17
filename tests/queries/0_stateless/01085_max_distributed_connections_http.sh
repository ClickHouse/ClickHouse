#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With `max_distributed_connections=2` and `max_threads=1` the two shards must still be read in parallel.
# Check it structurally instead of by wall time, which does not hold when the server is loaded: both remote
# queries have to be seen running at the same time. Read serially, they would never overlap.
# prefer_localhost_replica=0 forces real remote connections so the test exercises
# max_distributed_connections (parallel remote reads); otherwise on the macOS runner the
# lo0-aliased 127.0.0.2/3 are local and get executed serially under max_threads=1. See the
# note in 01085_max_distributed_connections.sh.

query_id="${CLICKHOUSE_DATABASE}_01085_http_${RANDOM}${RANDOM}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&max_distributed_connections=2&max_threads=1&prefer_localhost_replica=0" \
    -d "SELECT sleepEachRow(2) FROM remote('127.{2,3}', system.one) FORMAT Null" > /dev/null 2>&1 &
query_pid=$!

overlapped=0
while kill -0 "$query_pid" 2>/dev/null; do
    running=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE initial_query_id = '${query_id}' AND NOT is_initial_query")
    if [[ "$running" -ge 2 ]]; then
        overlapped=1
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE initial_query_id = '${query_id}' SYNC FORMAT Null"
wait "$query_pid" || true

[[ "$overlapped" -eq 1 ]] || echo "The remote queries did not run in parallel"
