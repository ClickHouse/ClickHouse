#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_POSTFIX=`random_str 10`
USER="test_user_02771_$USER_POSTFIX"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $USER"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO $USER"
$CLICKHOUSE_CLIENT -u "$USER" -q "SELECT * FROM system.numbers LIMIT 1"
$CLICKHOUSE_CLIENT -u "$USER" -q "SELECT * FROM system.numbers LIMIT 1"
$CLICKHOUSE_CLIENT -q "SELECT user, toBool(ProfileEvents['SelectQuery'] > 0), toBool(ProfileEvents['Query'] > 0) FROM system.user_processes WHERE user='default'"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['SelectQuery'], ProfileEvents['Query'] FROM system.user_processes WHERE user='$USER'"
$CLICKHOUSE_CLIENT -q "DROP USER $USER"

