#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.json

echo '{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : ["b1", "b2"], "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44, "h" : "2020-02-02 10:00:00"}}}' > $DATA_FILE

echo "JSON with no arguments"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from file($DATA_FILE, JSONAsObject)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json.a.b.c, json.b, json.^d from file($DATA_FILE, JSONAsObject)"

echo "JSON(a.b Tuple(c UInt32, d Array(Bool)), SKIP d.e, SKIP c, SKIP REGEXP '.*h.*')"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject, 'json JSON(a.b Tuple(c UInt32, d Array(Bool)), SKIP d.e, SKIP c, SKIP REGEXP \'.*h.*\')')"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from file($DATA_FILE, JSONAsObject, 'json JSON(a.b Tuple(c UInt32, d Array(Bool)), SKIP d.e, SKIP c, SKIP REGEXP \'.*h.*\')')"

echo "JSON(a.b.c UInt32, max_dynamic_paths=2)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_paths=2)')"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_paths=2)')"

echo "JSON(a.b.c UInt32, max_dynamic_paths=0)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_paths=0)')"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_paths=0)')"

echo "JSON(a.b.c UInt32, max_dynamic_types=1)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_types=0)')"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from file($DATA_FILE, JSONAsObject, 'json JSON(a.b.c UInt32, max_dynamic_types=0)')"

echo "Test small max_read_buffer_size"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --max_read_buffer_size=1 -q "select json from file($DATA_FILE, JSONAsObject)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --max_read_buffer_size=2 -q "select json from file($DATA_FILE, JSONAsObject)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --max_read_buffer_size=3 -q "select json from file($DATA_FILE, JSONAsObject)"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --max_read_buffer_size=4 -q "select json from file($DATA_FILE, JSONAsObject)"

echo "Test PrettyJSONEachRow"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, JSONAsObject) format PrettyJSONEachRow"

echo "Test TSV"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, TSV, 'json JSON') format TSV"
echo "Test CSV"
echo '"{""a"" : {""b"" : {""c"" : 1, ""d"" : [0, 1]}}, ""b"" : ""2020-01-01"", ""c"" : 42, ""d"" : {""e"" : {""f"" : [""s1"", ""s2""]}}}"
"{""a"" : {""b"" : {""c"" : 2, ""d"" : [2, 3]}}, ""b"" : [1, 2, 3], ""c"" : null, ""d"" : {""e"" : {""g"" : 43}}}"
"{""a"" : {""b"" : {""c"" : 3, ""d"" : [4, 5]}}, ""e"" : ""Hello, World!""}"
"{""a"" : {""b"" : {""c"" : 4, ""d"" : [6, 7]}}, ""c"" : 43}"
"{""a"" : {""b"" : {""c"" : 5, ""d"" : [8, 9]}}, ""b"" : [""b1"", ""b2""], ""d"" : {""e"" : {""f"" : [""s3"", ""s4""], ""g"" : 44, ""h"" : ""2020-02-02 10:00:00""}}}"' > $DATA_FILE
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select json from file($DATA_FILE, CSV, 'json JSON') format CSV"

rm $DATA_FILE
