#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rm -rf ${USER_FILES_PATH}/file_{0..10}.csv

echo '0' > ${USER_FILES_PATH}/file_0.csv
echo '0' > ${USER_FILES_PATH}/file_1.csv
echo '0' > ${USER_FILES_PATH}/file_2.csv
echo '0' > ${USER_FILES_PATH}/file_3.csv
echo '0' > ${USER_FILES_PATH}/file_4.csv
echo '0' > ${USER_FILES_PATH}/file_5.csv
echo '0' > ${USER_FILES_PATH}/file_6.csv
echo '0' > ${USER_FILES_PATH}/file_7.csv
echo '0' > ${USER_FILES_PATH}/file_8.csv
echo '0' > ${USER_FILES_PATH}/file_9.csv
echo '0' > ${USER_FILES_PATH}/file_10.csv

# echo '' > ${USER_FILES_PATH}/file_10.csv

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_regex (id UInt64) ENGINE = MergeTree() order by id;"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_regex SELECT * FROM file('file_{0..10}.csv','CSV');"
${CLICKHOUSE_CLIENT} -q "SELECT count() from t_regex;"

rm -rf ${USER_FILES_PATH}/file_{0..10}.csv;
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"
