#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

# Tests the override of certain MySQL proprietary settings to ClickHouse native settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CHANGED_SETTINGS_QUERY="SELECT name, value FROM system.settings WHERE name IN ('limit', 'send_timeout', 'receive_timeout') AND changed;"

echo "-- Uppercase tests"
${MYSQL_CLIENT} --execute "SET SQL_SELECT_LIMIT = 11; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "SET NET_WRITE_TIMEOUT = 22; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "SET NET_READ_TIMEOUT = 33; $CHANGED_SETTINGS_QUERY"

echo "-- Lowercase tests"
${MYSQL_CLIENT} --execute "set sql_select_limit=44; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "set net_write_timeout=55; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "set net_read_timeout=66; $CHANGED_SETTINGS_QUERY"
