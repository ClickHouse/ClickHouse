#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# see 01658_read_file_to_stringcolumn.sh
CLICKHOUSE_USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path, _file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

# Prepare data
unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
user_files_tmp_dir=${CLICKHOUSE_USER_FILES_PATH}/${unique_name}
mkdir -p ${user_files_tmp_dir}/tmp/
echo '"id","str","int","text"' > ${user_files_tmp_dir}/tmp.csv
echo '1,"abc",123,"abacaba"' >> ${user_files_tmp_dir}/tmp.csv
echo '2,"def",456,"bacabaa"' >> ${user_files_tmp_dir}/tmp.csv
echo '3,"story",78912,"acabaab"' >> ${user_files_tmp_dir}/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> ${user_files_tmp_dir}/tmp.csv

tmp_dir=${CLICKHOUSE_TEST_UNIQUE_NAME}

$CLICKHOUSE_LOCAL -q "insert into function file('$user_files_tmp_dir/tmp_numbers_1.csv') select * from numbers(1, 10)"
$CLICKHOUSE_LOCAL -q "insert into function file('$user_files_tmp_dir/tmp_numbers_2.csv') select * from numbers(11, 10)"
$CLICKHOUSE_LOCAL -q "insert into function file('$user_files_tmp_dir/tmp_numbers_30.csv') select * from numbers(21, 10)"

[[ -d $tmp_dir ]] && rm -rd $tmp_dir
mkdir $tmp_dir
cp ${user_files_tmp_dir}/tmp.csv ${tmp_dir}/tmp.csv
cp ${user_files_tmp_dir}/tmp.csv ${user_files_tmp_dir}/tmp/tmp.csv
cp ${user_files_tmp_dir}/tmp.csv ${user_files_tmp_dir}/tmp.myext
cp ${user_files_tmp_dir}/tmp_numbers_1.csv ${user_files_tmp_dir}/tmp/tmp_numbers_1.csv

#################
echo "Test 1: create filesystem database and check implicit calls"
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
CREATE DATABASE test1 ENGINE = Filesystem;
"""
echo $?
${CLICKHOUSE_CLIENT} --query "SHOW DATABASES" | grep "test1"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/tmp.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/tmp/tmp.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/tmp_numbers_*.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/tmp/*tmp_numbers_*.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/*/*tmp_numbers_*.csv\`;"
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT _path) FROM test1.\`${unique_name}/*.csv\` WHERE startsWith(_path, '${user_files_tmp_dir}')";
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT _path) FROM test1.\`${unique_name}/*.csv\` WHERE not startsWith(_path, '${user_files_tmp_dir}')";
# **/* does not search in the current directory but searches recursively in nested directories.
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT _path) FROM test1.\`${unique_name}/**/*.csv\` WHERE startsWith(_path, '${user_files_tmp_dir}')";
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT _path) FROM test1.\`${unique_name}/**/*.csv\` WHERE not startsWith(_path, '${user_files_tmp_dir}')";
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`tmp_numbers_*.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "CANNOT_EXTRACT_TABLE_STRUCTURE" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_LOCAL} -q "SELECT COUNT(*) FROM \"${tmp_dir}/tmp.csv\""

#################
echo "Test 2: check DatabaseFilesystem access rights and errors handling on server"
# DATABASE_ACCESS_DENIED: Allows list files only inside user_files
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../tmp.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`/tmp/tmp.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../*/tmp_numbers_*.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../tmp_numbers_*.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../*.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --multiline --multiquery --query """
USE test1;
SELECT COUNT(*) FROM \"../${tmp_dir}/tmp.csv\";
""" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`../../../../../../tmp.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF "PATH_ACCESS_DENIED" > /dev/null && echo "OK" || echo 'FAIL' ||:

# BAD_ARGUMENTS: path should be inside user_files
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = Filesystem('/tmp');
""" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:

# BAD_ARGUMENTS: .../user_files/relative_unknown_dir does not exists
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = Filesystem('relative_unknown_dir');
""" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:

# FILE_DOESNT_EXIST: unknown file
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`tmp2.csv\`;" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "FILE_DOESNT_EXIST" > /dev/null && echo "OK" || echo 'FAIL' ||:

# BAD_ARGUMENTS: Cannot determine the file format by it's extension
${CLICKHOUSE_CLIENT} --query "SELECT COUNT(*) FROM test1.\`${unique_name}/tmp.myext\`;" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:
# Clean
${CLICKHOUSE_CLIENT} --query "DROP DATABASE test1;"
rm -rd $tmp_dir
rm -rd $user_files_tmp_dir
