#!/usr/bin/env bash
# Tags: distributed, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


i=0 retries=10
# Sometimes the query takes longer than expected due to system overload.
# Reduced sleep(1)->sleep(0.1) and retries 60->10 to keep worst case under 30s.
while [[ $i -lt $retries ]]; do
    timeout 3s ${CLICKHOUSE_CLIENT} --max_threads 10 --query "SELECT * FROM url('http://127.0.0.{1..10}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+sleep(0.1)', TSV, 'x UInt8')" --format Null && break
    ((++i))
done
