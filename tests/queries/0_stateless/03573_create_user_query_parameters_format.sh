#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_FORMAT <<<"CREATE USER {username:Identifier} IDENTIFIED WITH no_password"
$CLICKHOUSE_FORMAT <<<"CREATE USER {username:Identifier}@'clickhouse.com' IDENTIFIED WITH no_password"
$CLICKHOUSE_FORMAT <<<"CREATE USER {username:Identifier}@'127.0.0.1' IDENTIFIED WITH no_password"
$CLICKHOUSE_FORMAT <<<"CREATE USER {username:Identifier}@'192.168.0.1' IDENTIFIED WITH no_password"
$CLICKHOUSE_FORMAT <<<"CREATE USER foo@'127.0.0.1' IDENTIFIED WITH no_password"
