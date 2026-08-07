#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q $'create table test (`x.a` UInt32, `x.b` UInt32) engine=Memory'

echo '{"x" : {"a" : 1, "b" : 2}}' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+test+FORMAT+JSONEachRow&input_format_import_nested_json=1&max_threads=10&input_format_parallel_parsing=0"

echo '{"x" : {"a" : 3, "b" : 4}}' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+test+FORMAT+JSONEachRow&input_format_import_nested_json=1&max_threads=10&input_format_parallel_parsing=1"

$CLICKHOUSE_CLIENT -q $'select 5 as `x.a`, 6 as `x.b` format BSONEachRow' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+test+FORMAT+BSONEachRow&max_threads=10&input_format_parallel_parsing=0"


$CLICKHOUSE_CLIENT -q $'select 7 as `x.a`, 8 as `x.b` format BSONEachRow' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+test+FORMAT+BSONEachRow&max_threads=10&input_format_parallel_parsing=1"

$CLICKHOUSE_CLIENT -q "select * from test order by 1 format Vertical";
$CLICKHOUSE_CLIENT -q "drop table test";

