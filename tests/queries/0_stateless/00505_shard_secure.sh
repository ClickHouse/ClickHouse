#!/usr/bin/env bash

# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}:$CLICKHOUSE_PORT_TCP_SECURE', system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure(test_shard_localhost_secure, system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remote(test_shard_localhost_secure, system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure(test_shard_localhost, system.one);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM remote(test_shard_localhost, system.one);"

