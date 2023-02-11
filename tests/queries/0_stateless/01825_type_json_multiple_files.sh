#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_files_path=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')
for f in "$user_files_path"/01825_file_*.json; do
    [ -e $f ] && rm $f
done

for i in {0..5}; do
    echo "{\"k$i\": 100}" > "$user_files_path"/01825_file_$i.json
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_files"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_files (file String, data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_files SELECT _file, data FROM file('01825_file_*.json', 'JSONAsObject', 'data JSON')"

${CLICKHOUSE_CLIENT} -q "SELECT data FROM t_json_files ORDER BY file FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
${CLICKHOUSE_CLIENT} -q "SELECT toTypeName(data) FROM t_json_files LIMIT 1"

${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE IF EXISTS t_json_files"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_files \
    SELECT _file, data FROM file('01825_file_*.json', 'JSONAsObject', 'data JSON') \
    ORDER BY _file LIMIT 3" --max_threads 1 --min_insert_block_size_rows 1 --max_insert_block_size 1 --max_block_size 1

${CLICKHOUSE_CLIENT} -q "SELECT data FROM t_json_files ORDER BY file FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
${CLICKHOUSE_CLIENT} -q "SELECT toTypeName(data) FROM t_json_files LIMIT 1"

${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE IF EXISTS t_json_files"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_files \
    SELECT _file, data FROM file('01825_file_*.json', 'JSONAsObject', 'data JSON') \
    WHERE _file IN ('01825_file_1.json', '01825_file_3.json')"

${CLICKHOUSE_CLIENT} -q "SELECT data FROM t_json_files ORDER BY file FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
${CLICKHOUSE_CLIENT} -q "SELECT toTypeName(data) FROM t_json_files LIMIT 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_files"
rm "$user_files_path"/01825_file_*.json
