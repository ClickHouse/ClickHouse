#!/usr/bin/env bash

# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Not default server config needed

tcp_port_secure=`$CLICKHOUSE_EXTRACT_CONFIG -k tcp_port_secure 2>/dev/null`
if [ -z $tcp_port_secure ]; then
    # Secure port disabled. Fake result
    cat $CURDIR/00505_shard_secure.reference
else

    $CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}:$CLICKHOUSE_PORT_TCP_SECURE', system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure(test_shard_localhost_secure, system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote(test_shard_localhost_secure, system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remoteSecure(test_shard_localhost, system.one);"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM remote(test_shard_localhost, system.one);"

fi
