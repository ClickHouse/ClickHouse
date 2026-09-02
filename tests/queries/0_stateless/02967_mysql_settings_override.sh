#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

# Tests that certain MySQL-proprietary settings are mapped to ClickHouse-native settings.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CHANGED_SETTINGS_QUERY="SELECT name, value FROM system.settings WHERE name IN ('send_timeout', 'receive_timeout') AND changed;"

TEST_TABLE="mysql_settings_override_test"

DROP_TABLE="DROP TABLE IF EXISTS $TEST_TABLE;"
CREATE_TABLE="CREATE TABLE $TEST_TABLE (s String) ENGINE MergeTree ORDER BY s;"
INSERT_STMT="INSERT INTO $TEST_TABLE VALUES ('a'), ('b'), ('c'), ('d');"
SELECT_STMT="SELECT * FROM $TEST_TABLE ORDER BY s;"

echo "-- Init"
${MYSQL_CLIENT} --execute "$DROP_TABLE $CREATE_TABLE $INSERT_STMT $SELECT_STMT" # should fetch all 4 records

echo "-- Uppercase setting name"
${MYSQL_CLIENT} --execute "SET SQL_SELECT_LIMIT = 2; $SELECT_STMT" # should fetch 2 records out of 4
${MYSQL_CLIENT} --execute "SET NET_WRITE_TIMEOUT = 22; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "SET NET_READ_TIMEOUT = 33; $CHANGED_SETTINGS_QUERY"

echo "-- Lowercase setting name"
${MYSQL_CLIENT} --execute "set sql_select_limit=3; $SELECT_STMT" # should fetch 3 records out of 4
${MYSQL_CLIENT} --execute "set net_write_timeout=55; $CHANGED_SETTINGS_QUERY"
${MYSQL_CLIENT} --execute "set net_read_timeout=66; $CHANGED_SETTINGS_QUERY"

${MYSQL_CLIENT} --execute "$DROP_TABLE"
