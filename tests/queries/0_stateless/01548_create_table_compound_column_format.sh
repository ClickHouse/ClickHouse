#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo "CREATE TABLE test(a Int64, b NESTED(a Int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT

echo "CREATE TABLE test(a Int64, b TUPLE(a Int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT