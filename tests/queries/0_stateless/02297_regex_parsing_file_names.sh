#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.

# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
CLICKHOUSE_USER_FILES_PATH=$(clickhouse-client --query "select _path, _file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p ${CLICKHOUSE_USER_FILES_PATH}/

rm -rf ${CLICKHOUSE_USER_FILES_PATH}/file_{0..10}.json

echo '{"obj": "aaa", "id": 1, "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_0.json
echo '{"id": 2, "obj": "bbb", "s": "bar"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_1.json
echo '{"id": 3, "obj": "ccc", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_2.json
echo '{"id": 4, "obj": "ddd", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_3.json
echo '{"id": 5, "obj": "eee", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_4.json
echo '{"id": 6, "obj": "fff", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_5.json
echo '{"id": 7, "obj": "ggg", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_6.json
echo '{"id": 8, "obj": "hhh", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_7.json
echo '{"id": 9, "obj": "iii", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_8.json
echo '{"id": 10, "obj":"jjj", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_9.json
echo '{"id": 11, "obj": "kkk", "s": "foo"}' > ${CLICKHOUSE_USER_FILES_PATH}/file_10.json

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_regex (id UInt64, obj String, s String) ENGINE = MergeTree() order by id;"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_regex SELECT * FROM file('file_{0..10}.json','JSONEachRow');"
${CLICKHOUSE_CLIENT} -q "SELECT count() from t_regex;"

rm -rf ${CLICKHOUSE_USER_FILES_PATH}/file_{0..10}.json;
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"
