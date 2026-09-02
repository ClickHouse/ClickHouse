#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '{"t" : {"a" : 1, "b" : 2}}' | $CLICKHOUSE_LOCAL --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table"

echo '{"t" : { "a" : 1 , "b" : 2 } }' | $CLICKHOUSE_LOCAL --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table"

echo '{"t" : {}}' | $CLICKHOUSE_LOCAL --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table"

echo '{"t" : {"a" : 1, "b" : 2}}' | $CLICKHOUSE_LOCAL --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table" --input_format_json_defaults_for_missing_elements_in_named_tuple=0 2>&1 | grep -F "INCORRECT_DATA" -c

echo '{"t" : {"a" : 1, "d" : 2}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=0 --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table" 2>&1 | grep -F "NOT_FOUND_COLUMN_IN_BLOCK" -c

echo '{"t" : {"a" : 1, "b" : 2, "c" : 3, "d" : 4}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=0 --input-format=NDJSON --structure='t Tuple(a Nullable(UInt32), b Nullable(UInt32), c Nullable(UInt32))' -q "select * from table" 2>&1 | grep -F "INCORRECT_DATA" -c

