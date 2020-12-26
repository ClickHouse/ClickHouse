#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --iterations 10 --query "SELECT 1" 1>/dev/null 2>"$CLICKHOUSE_TMP"/err

cat "$CLICKHOUSE_TMP"/err | grep Exception
cat "$CLICKHOUSE_TMP"/err | grep Loaded

rm "$CLICKHOUSE_TMP"/err
