#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# triggered not for the first query
yes "select * from remote('127.{2,3}', system.numbers) where number = 10 limit 1;" 2>/dev/null | {
    head -n20 | ${CLICKHOUSE_CLIENT} -n 2>&1 >/dev/null | grep 'DB::Exception: '
}
# grep above will fail during diff with .reference
exit 0
