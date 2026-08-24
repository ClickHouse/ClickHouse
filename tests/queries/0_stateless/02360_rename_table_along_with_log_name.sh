#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ ! -z "$CLICKHOUSE_CLIENT_REDEFINED" ] && CLICKHOUSE_CLIENT=$CLICKHOUSE_CLIENT_REDEFINED

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS x;"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS y;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE x(i int) ENGINE MergeTree ORDER BY i;"
$CLICKHOUSE_CLIENT -q "RENAME TABLE x TO y;"

CLICKHOUSE_CLIENT_WITH_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')
regexp="${CLICKHOUSE_DATABASE}\\.x" # Check if there are still log entries with old table name
$CLICKHOUSE_CLIENT_WITH_LOG --send_logs_source_regexp "$regexp" -q "INSERT INTO y VALUES(1);"

$CLICKHOUSE_CLIENT -q "DROP TABLE y;"
