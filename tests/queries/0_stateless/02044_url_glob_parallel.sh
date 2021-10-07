#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Sometimes five seconds are not enough due to system overload.
# But if it can run in less than five seconds at least sometimes - it is enough for the test.
while true
do
    timeout 5s ${CLICKHOUSE_CLIENT} --max_threads 10 --query "SELECT * FROM url('http://127.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(1)', TSV, 'x UInt8')" --format Null && break
done
