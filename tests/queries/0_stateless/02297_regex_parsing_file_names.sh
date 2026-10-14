#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rm -rf ${CLICKHOUSE_USER_FILES_UNIQUE}/file_{0..10}.csv

echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_0.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_1.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_2.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_3.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_4.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_5.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_6.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_7.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_8.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_9.csv
echo '0' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_10.csv

# echo '' > ${CLICKHOUSE_USER_FILES_UNIQUE}/file_10.csv

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_regex (id UInt64) ENGINE = MergeTree() order by id;"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_regex SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/file_{0..10}.csv','CSV');"
${CLICKHOUSE_CLIENT} -q "SELECT count() from t_regex;"

rm -rf ${CLICKHOUSE_USER_FILES_UNIQUE}/file_{0..10}.csv;
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_regex;"
