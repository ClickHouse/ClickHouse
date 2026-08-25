#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# With show_processlist_include_internal = 0, the internal query should not be shown.
output=$($CLICKHOUSE_CLIENT \
    --query 'SHOW PROCESSLIST SETTINGS show_processlist_include_internal = 0' \
    --format 'JSONEachRow' \
    | jq "select((.query | contains(\"system.processes\")) and .current_database == \"${CLICKHOUSE_DATABASE}\" and .is_internal == 1)")

[ -z "$output" ] && echo 'not found' || echo 'found'

# With show_processlist_include_internal = 1 (default), the internal query should be shown.
output=$($CLICKHOUSE_CLIENT \
    --query 'SHOW PROCESSLIST' \
    --format 'JSONEachRow' \
    | jq "select((.query | contains(\"system.processes\")) and .current_database == \"${CLICKHOUSE_DATABASE}\" and .is_internal == 1)")

[ -z "$output" ] && echo 'not found' || echo 'found'
