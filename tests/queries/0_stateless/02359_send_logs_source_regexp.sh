#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ ! -z "$CLICKHOUSE_CLIENT_REDEFINED" ] && CLICKHOUSE_CLIENT=$CLICKHOUSE_CLIENT_REDEFINED

regexp="executeQuery|InterpreterSelectQuery"
$CLICKHOUSE_CLIENT --send_logs_source_regexp "$regexp" -q "SELECT 1;" 2> >(grep -v -E "$regexp" 1>&2)
