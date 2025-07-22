#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.data

$CLICKHOUSE_LOCAL -q "select tuple(1, x'00000000000000000000FFFF0000000000') as x format BSONEachRow" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', BSONEachRow, 'x Tuple(UInt32, IPv6)') settings input_format_allow_errors_num=1"

$CLICKHOUSE_LOCAL -q "select [x'00000000000000000000FFFF00000000', x'00000000000000000000FFFF0000000000'] as x format BSONEachRow" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', BSONEachRow, 'x Array(IPv6)') settings input_format_allow_errors_num=1"

$CLICKHOUSE_LOCAL -q "select map('key1', x'00000000000000000000FFFF00000000', 'key2', x'00000000000000000000FFFF0000000000') as x format BSONEachRow" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', BSONEachRow, 'x Map(String, IPv6)') settings input_format_allow_errors_num=1"

rm $DATA_FILE
