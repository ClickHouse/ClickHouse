#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# see 01658_read_file_to_stringcolumn.sh
CLICKHOUSE_USER_FILES_PATH=$(clickhouse-client --query "select _path, _file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

# Prepare data
mkdir -p ${CLICKHOUSE_USER_FILES_PATH}/tmp/
echo '"id","str","int","text"' > ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '1,"abc",123,"abacaba"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '2,"def",456,"bacabaa"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '3,"story",78912,"acabaab"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv

tmp_dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
[[ -d $tmp_dir ]] && rm -rd $tmp_dir
mkdir $tmp_dir
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${tmp_dir}/tmp.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp/tmp.csv

#################
echo "Test 1: create filesystem database and check implicit calls"
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
CREATE DATABASE test1 ENGINE = Filesystem;
"""
echo $?
${CLICKHOUSE_CLIENT} --query "SHOW DATABASES" | grep "test1"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`tmp.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`tmp/tmp.csv\`;"
${CLICKHOUSE_LOCAL} -q "SELECT COUNT(*) FROM \"${tmp_dir}/tmp.csv\""

#################
echo "Test 2: check DatabaseFilesystem access rights on server"
# Allows list files only inside user_files
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../tmp.csv\`;" 2>&1| grep -F "Code: 291" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`/tmp/tmp.csv\`;" 2>&1| grep -F "Code: 291" > /dev/null && echo "OK"

${CLICKHOUSE_CLIENT} --multiline --multiquery --query """
USE test1;
SELECT COUNT(*) FROM \"../${tmp_dir}/tmp.csv\";
""" 2>&1| grep -F "Code: 291" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../../../../../../tmp.csv\`;" 2>&1| grep -F "Code: 291" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = Filesystem('/tmp');
SELECT COUNT(*) FROM test2.\`tmp.csv\`;
""" 2>&1| grep -F "Code: 291" > /dev/null && echo "OK"

# Clean
${CLICKHOUSE_CLIENT} --query "DROP DATABASE test1;"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE test2;"
rm -rd $tmp_dir
rm -rd $CLICKHOUSE_USER_FILES_PATH
