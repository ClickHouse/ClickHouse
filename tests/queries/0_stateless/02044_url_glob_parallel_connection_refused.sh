#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


i=0 retries=5
client_opts=(
    --http_max_tries 1
    --max_execution_time 3
    --max_threads 10
    --query "SELECT * FROM url('http://128.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(1)', TSV, 'x UInt8')"
    --format Null
)
# Connecting to wrong address and checking for race condition
while [[ $i -lt $retries ]]; do
    clickhouse_client_timeout 4s ${CLICKHOUSE_CLIENT} "${client_opts[@]}" 2>/dev/null
    ((++i))
done
