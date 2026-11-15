#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select number as x from numbers(5) format JSONEachRow" > $CLICKHOUSE_TEST_UNIQUE_NAME.jsonl
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl', auto, 'x UInt64 Ephemeral, y UInt64 default x + 1')" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl', auto, 'x UInt64 Alias y, y UInt64')" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl', auto, 'x UInt64 Materialized 42, y UInt64')" 2>&1 | grep -c "BAD_ARGUMENTS"

$CLICKHOUSE_LOCAL -q "
    create table test (x UInt64 Ephemeral, y UInt64 default x + 1) engine=Memory;
    insert into test (x, y) select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl');
    select * from test;
    truncate table test;
    insert into test (x, y) from infile '$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl';
    select * from test
"

rm  $CLICKHOUSE_TEST_UNIQUE_NAME.jsonl

