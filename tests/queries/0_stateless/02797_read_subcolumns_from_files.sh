#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME

$CLICKHOUSE_LOCAL -q "select ((1, 2), 3)::Tuple(b Tuple(c UInt32, d UInt32), e UInt32) as a format TSV" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select a.b.d,  a.b, a.e from file('$DATA_FILE', TSV, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"

$CLICKHOUSE_LOCAL -q "select ((1, 2), 3)::Tuple(b Tuple(c UInt32, d UInt32), e UInt32) as a format JSONEachRow" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select a.b.d, a.b, a.e from file('$DATA_FILE', JSONEachRow, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
$CLICKHOUSE_LOCAL -q "select x.b.d, x.b, x.e from file('$DATA_FILE', JSONEachRow, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
$CLICKHOUSE_LOCAL -q "select x.b.d, x.b, x.e from file('$DATA_FILE', JSONEachRow, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)')"

rm $DATA_FILE

