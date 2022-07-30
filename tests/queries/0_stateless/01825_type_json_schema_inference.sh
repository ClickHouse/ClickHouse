#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_inference"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_inference (id UInt64, obj Object(Nullable('json')), s String) \
    ENGINE = MergeTree ORDER BY id" --allow_experimental_object_type 1

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

filename="${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data.json"

echo '{"id": 1, "obj": {"k1": 1, "k2": {"k3": 2, "k4": [{"k5": 3}, {"k5": 4}]}}, "s": "foo"}' > $filename
echo '{"id": 2, "obj": {"k2": {"k3": "str", "k4": [{"k6": 55}]}, "some": 42}, "s": "bar"}' >> $filename

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_inference SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.json', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_json_inference FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
${CLICKHOUSE_CLIENT} -q "SELECT toTypeName(obj) FROM t_json_inference LIMIT 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_inference"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_inference (id UInt64, obj String, s String) ENGINE = MergeTree ORDER BY id"

echo '{"obj": "aaa", "id": 1, "s": "foo"}' > $filename
echo '{"id": 2, "obj": "bbb", "s": "bar"}' >> $filename

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_inference SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.json', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_json_inference FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_inference"

echo '{"map": {"k1": 1, "k2": 2}, "obj": {"k1": 1, "k2": {"k3": 2}}}' > $filename

${CLICKHOUSE_CLIENT} -q "SELECT map, obj, toTypeName(map) AS map_type, toTypeName(obj) AS obj_type \
    FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.json', 'JSONEachRow') FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_inference (obj JSON, map Map(String, UInt64)) \
    ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

echo '{"map": {"k1": 1, "k2": 2}, "obj": {"k1": 1, "k2": 2}}' > $filename

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_inference SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.json', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_json_inference FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
${CLICKHOUSE_CLIENT} -q "SELECT toTypeName(obj) FROM t_json_inference LIMIT 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_inference"
