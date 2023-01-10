#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "allow_settings_after_format_in_insert"
echo "22.3"
$CLICKHOUSE_CLIENT --compatibility=22.3 -q "select value from system.settings where name='allow_settings_after_format_in_insert'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compatibility=22.3" -d "select value from system.settings where name='allow_settings_after_format_in_insert'"
echo "22.4"
$CLICKHOUSE_CLIENT --compatibility=22.4 -q "select value from system.settings where name='allow_settings_after_format_in_insert'"
echo "22.5"
$CLICKHOUSE_CLIENT --compatibility=22.5 -q "select value from system.settings where name='allow_settings_after_format_in_insert'"


echo "async_socket_for_remote"
echo "21.2"
$CLICKHOUSE_CLIENT --compatibility=21.2 -q "select value from system.settings where name='async_socket_for_remote'"
echo "21.3"
$CLICKHOUSE_CLIENT --compatibility=21.3 -q "select value from system.settings where name='async_socket_for_remote'"
echo "21.4"
$CLICKHOUSE_CLIENT --compatibility=21.4 -q "select value from system.settings where name='async_socket_for_remote'"
echo "21.5"
$CLICKHOUSE_CLIENT --compatibility=21.5 -q "select value from system.settings where name='async_socket_for_remote'"
echo "21.6"
$CLICKHOUSE_CLIENT --compatibility=21.6 -q "select value from system.settings where name='async_socket_for_remote'"

