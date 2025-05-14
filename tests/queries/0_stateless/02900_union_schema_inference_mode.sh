#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p $CLICKHOUSE_TEST_UNIQUE_NAME
echo '{"a" : 1, "obj" : {"f1" : 1, "f2" : "2020-01-01"}}' > $CLICKHOUSE_TEST_UNIQUE_NAME/data1.jsonl
echo '{"b" : 2, "obj" : {"f3" : 2, "f2" : "Some string"}}' > $CLICKHOUSE_TEST_UNIQUE_NAME/data2.jsonl
echo '{"c" : "hello"}' > $CLICKHOUSE_TEST_UNIQUE_NAME/data3.jsonl

$CLICKHOUSE_LOCAL -m -q "
set schema_inference_mode = 'union';
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2,3}.jsonl');
select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2,3}.jsonl') order by tuple(*) format JSONEachRow;
select schema_inference_mode, splitByChar('/', source)[-1] as file, schema from system.schema_inference_cache order by file;
"

$CLICKHOUSE_LOCAL -m -q "
set schema_inference_mode = 'union';
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data3.jsonl');
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2,3}.jsonl');
"

cd $CLICKHOUSE_TEST_UNIQUE_NAME/ && tar -cf archive.tar data1.jsonl data2.jsonl data3.jsonl && cd ..

$CLICKHOUSE_LOCAL -m -q "
set schema_inference_mode = 'union';
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/archive.tar :: data{1,2,3}.jsonl');
select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME/archive.tar :: data{1,2,3}.jsonl') order by tuple(*) format JSONEachRow;
select schema_inference_mode, splitByChar('/', source)[-1] as file, schema from system.schema_inference_cache order by file;
"

$CLICKHOUSE_LOCAL -m -q "
set schema_inference_mode = 'union';
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/archive.tar :: data3.jsonl');
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/archive.tar :: data{1,2,3}.jsonl');
"

echo 'Error' > $CLICKHOUSE_TEST_UNIQUE_NAME/data4.jsonl
$CLICKHOUSE_LOCAL -q "desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2,3,4}.jsonl') settings schema_inference_mode='union'" 2>&1 | grep -c -F "CANNOT_EXTRACT_TABLE_STRUCTURE"

$CLICKHOUSE_LOCAL -m -q "
set schema_inference_mode = 'union';
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{2,3}.jsonl');
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2,3,4}.jsonl');
" 2>&1 | grep -c -F "CANNOT_EXTRACT_TABLE_STRUCTURE"

echo 42 > $CLICKHOUSE_TEST_UNIQUE_NAME/data1.csv
echo 42, 43 > $CLICKHOUSE_TEST_UNIQUE_NAME/data2.csv

$CLICKHOUSE_LOCAL -q "desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2}.csv') settings schema_inference_mode='union'" 2>&1 | grep -c -F "BAD_ARGUMENTS";

rm -rf ${CLICKHOUSE_TEST_UNIQUE_NAME}

