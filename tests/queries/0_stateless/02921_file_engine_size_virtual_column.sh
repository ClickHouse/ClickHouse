#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1" > $CLICKHOUSE_TEST_UNIQUE_NAME.data1.tsv
echo "12" > $CLICKHOUSE_TEST_UNIQUE_NAME.data2.tsv
echo "123" > $CLICKHOUSE_TEST_UNIQUE_NAME.data3.tsv

$CLICKHOUSE_LOCAL -q "select _size from file('$CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv') order by _size"
# Run this query twice to check correct behaviour when cache is used
$CLICKHOUSE_LOCAL -q "select _size from file('$CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv') order by _size"

# Test the same fils in archive
tar -cf $CLICKHOUSE_TEST_UNIQUE_NAME.archive.tar $CLICKHOUSE_TEST_UNIQUE_NAME.data1.tsv $CLICKHOUSE_TEST_UNIQUE_NAME.data2.tsv $CLICKHOUSE_TEST_UNIQUE_NAME.data3.tsv

$CLICKHOUSE_LOCAL -q "select _size from file('$CLICKHOUSE_TEST_UNIQUE_NAME.archive.tar :: $CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv') order by _size"
$CLICKHOUSE_LOCAL -q "select _size from file('$CLICKHOUSE_TEST_UNIQUE_NAME.archive.tar :: $CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv') order by _size"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.*

