#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


i=0 retries=60
# Sometimes five seconds are not enough due to system overload.
# But if it can run in less than five seconds at least sometimes - it is enough for the test.
while [[ $i -lt $retries ]]; do
    timeout 5s ${CLICKHOUSE_CLIENT} --max_threads 10 --query "SELECT * FROM url('http://127.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(1)', TSV, 'x UInt8')" --format Null && break
    ((++i))
done
