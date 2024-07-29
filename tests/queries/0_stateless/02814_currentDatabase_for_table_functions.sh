#!/usr/bin/env bash

# Based on https://github.com/ClickHouse/ClickHouse/issues/52436
# Test that inserts performed via Buffer table engine land into destination table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --ignore-error --multiquery --query "DROP TABLE IF EXISTS null_table;
    DROP TABLE IF EXISTS null_table_buffer; DROP TABLE IF EXISTS null_mv; DROP VIEW IF EXISTS number_view;"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE null_table (number UInt64) ENGINE = Null;"
${CLICKHOUSE_CLIENT} --query="CREATE VIEW number_view as SELECT * FROM numbers(10) as tb;"
${CLICKHOUSE_CLIENT} --query="CREATE MATERIALIZED VIEW null_mv Engine = Log AS SELECT * FROM null_table LEFT JOIN number_view as tb USING number;"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE null_table_buffer (number UInt64) ENGINE = Buffer(currentDatabase(), null_table, 1, 1, 1, 100, 200, 10000, 20000);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO null_table_buffer VALUES (1);"

# Insert about should've landed into `null_mv`
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM null_mv;" | grep -q "1" && echo 'OK' && break
    sleep .5
done

${CLICKHOUSE_CLIENT} --query="SELECT * FROM null_mv;"