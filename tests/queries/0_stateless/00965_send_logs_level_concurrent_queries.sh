#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT * from numbers(1000000);" 2>&1 | awk '{ print $8 }' | grep -q "Trace" && echo "OK" || echo "Fail" &
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query="SELECT * from numbers(1000000);" 2>&1 | awk '{ print $8 }' | grep "Debug\|Trace" &
done

wait
