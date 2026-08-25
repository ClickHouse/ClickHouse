#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101748

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect <<< 'SELECT 1' >/dev/null 2>&1
echo "bare: $?"

$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect=0 <<< 'SELECT 1' >/dev/null 2>&1
echo "zero: $?"

$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect=3 <<< 'SELECT 1' >/dev/null 2>&1
echo "n: $?"
