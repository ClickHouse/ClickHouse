#!/usr/bin/env bash

# level should be 'trace', otherwise it will not trigger the issue
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# triggered not for the first query
for _ in {1..20}; do
    echo "select * from remote('127.{2,3}', system.numbers) where number = 10 limit 1;"
done | ${CLICKHOUSE_CLIENT} -n 2>/dev/null
