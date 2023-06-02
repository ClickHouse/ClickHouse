#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02771"
$CLICKHOUSE_CLIENT -q "CREATE USER test_user_02771"
$CLICKHOUSE_CLIENT -u test_user_02771 -q "SELECT * FROM system.numbers LIMIT 1"
$CLICKHOUSE_CLIENT -u test_user_02771 -q "SELECT * FROM system.numbers LIMIT 1"
$CLICKHOUSE_CLIENT -q "SELECT user FROM system.user_processes"
$CLICKHOUSE_CLIENT -q "SELECT user, toBool(ProfileEvents['SelectQuery'] > 0), toBool(ProfileEvents['Query'] > 0) FROM system.user_processes WHERE user='default'"
$CLICKHOUSE_CLIENT -q "SELECT user, ProfileEvents['SelectQuery'], ProfileEvents['Query'] FROM system.user_processes WHERE user='test_user_02771'"
$CLICKHOUSE_CLIENT -q "DROP USER test_user_02771"

