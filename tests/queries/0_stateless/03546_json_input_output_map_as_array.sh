#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a int, b Map(Tuple(UInt32, UInt32), Int32), c Map(String, UInt32)) ENGINE=MergeTree() Order by a;
INSERT INTO test SELECT 1, map((1,1),1,(2,2),2), map('b',1, 'c', 2);"

$CLICKHOUSE_CLIENT -n -q "
SELECT * 
FROM test LIMIT 1
FORMAT JSON SETTINGS output_format_json_pretty_print=0, output_format_json_map_as_array_of_tuples=1, output_format_json_named_tuples_as_objects=0, output_format_write_statistics=0;"

$CLICKHOUSE_CLIENT -n -q "
SELECT * 
FROM test LIMIT 1
FORMAT JSON SETTINGS output_format_json_pretty_print=1, output_format_json_map_as_array_of_tuples=1, output_format_json_named_tuples_as_objects=0, output_format_write_statistics=0;"


$CLICKHOUSE_CLIENT -n -q "
SELECT * 
FROM test  LIMIT 1
FORMAT JSON SETTINGS output_format_json_pretty_print=0, output_format_json_map_as_array_of_tuples=1, output_format_json_named_tuples_as_objects=0, output_format_write_statistics=0;"

$CLICKHOUSE_CLIENT -n -q "
SELECT * 
FROM test LIMIT 1
FORMAT JSON SETTINGS output_format_json_pretty_print=0, output_format_json_map_as_array_of_tuples=1, output_format_json_named_tuples_as_objects=0, output_format_write_statistics=0;" | 
$CLICKHOUSE_CLIENT -n -q "
INSERT INTO test SETTINGS input_format_json_map_as_array_of_tuples=1, input_format_json_named_tuples_as_objects=0 FORMAT JSON"

$CLICKHOUSE_CLIENT -n -q "SELECT count(*) FROM test;"


$CLICKHOUSE_CLIENT -n -q "
SELECT * 
FROM test LIMIT 2
FORMAT JSON SETTINGS output_format_json_pretty_print=0, output_format_json_map_as_array_of_tuples=1, output_format_json_named_tuples_as_objects=0, output_format_write_statistics=0;" | 
$CLICKHOUSE_CLIENT -n -q "
INSERT INTO test SETTINGS input_format_json_map_as_array_of_tuples=1, input_format_json_named_tuples_as_objects=0 FORMAT JSON"

$CLICKHOUSE_CLIENT -n -q "SELECT count(*) FROM test;"
