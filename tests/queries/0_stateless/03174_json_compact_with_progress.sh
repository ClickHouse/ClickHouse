#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table;"

$CLICKHOUSE_CLIENT -q "SELECT 1;"
# Check JSONCompactWithProgress Output 
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_table FORMAT JSONCompactWithProgress settings max_block_size=2;" |  grep -v --text "progress" | sed -E 's/"elapsed":[0-9]+\.[0-9]+/"elapsed":ELAPSED/g'

$CLICKHOUSE_CLIENT -q "SELECT 2;"
# Check Totals 
$CLICKHOUSE_CLIENT -q "SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactWithProgress settings max_block_size=2;" | grep -v --text "progress" | sed -E 's/"elapsed":[0-9]+\.[0-9]+/"elapsed":ELAPSED/g'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table;"
