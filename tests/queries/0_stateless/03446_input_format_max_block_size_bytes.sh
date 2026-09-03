#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select number from numbers(1000000) format RowBinary" > $CLICKHOUSE_TEST_UNIQUE_NAME.bin
$CLICKHOUSE_LOCAL -q "select count(distinct(blockNumber())) from file('$CLICKHOUSE_TEST_UNIQUE_NAME.bin', RowBinary, 'x UInt64') settings input_format_max_block_size_bytes=1000000, max_block_size=1000000"
$CLICKHOUSE_LOCAL -q "select count(distinct(blockNumber())) from file('$CLICKHOUSE_TEST_UNIQUE_NAME.bin', RowBinary, 'x UInt64') settings input_format_max_block_size_bytes=100000, max_block_size=1000000"
$CLICKHOUSE_LOCAL -q "select count(distinct(blockNumber())) from file('$CLICKHOUSE_TEST_UNIQUE_NAME.bin', RowBinary, 'x UInt64') settings input_format_max_block_size_bytes=10000, max_block_size=1000000"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.bin

