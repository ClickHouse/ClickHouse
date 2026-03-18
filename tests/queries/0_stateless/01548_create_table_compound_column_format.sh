#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "CREATE TABLE test(a Int64, b Nested(a Int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT

echo "CREATE TABLE test(a Int64, b Tuple(a Int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT