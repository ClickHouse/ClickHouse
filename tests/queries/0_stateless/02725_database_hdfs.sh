#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs, no-parallel

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prepare data
${CLICKHOUSE_CLIENT} -q "insert into table function hdfs('hdfs://localhost:12222/test_02725_1.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 1, 2, 3 settings hdfs_truncate_on_insert=1;"
ret=$?
if [ $ret -ne 0 ]; then
    echo "Insert failed!"
    exit 1
fi
${CLICKHOUSE_CLIENT} -q "insert into table function hdfs('hdfs://localhost:12222/test_02725_2.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 4, 5, 6 settings hdfs_truncate_on_insert=1;"
ret=$?
if [ $ret -ne 0 ]; then
    echo "Insert failed!"
    exit 1
fi

#################
echo "Test 1: select from hdfs database"

# Database without specific host
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test_hdfs_1;
CREATE DATABASE test_hdfs_1 ENGINE = HDFS;
USE test_hdfs_1;
SELECT * FROM \"hdfs://localhost:12222/test_02725_1.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test_hdfs_1

# Database with host
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test_hdfs_2;
CREATE DATABASE test_hdfs_2 ENGINE = HDFS('hdfs://localhost:12222');
USE test_hdfs_2;
SELECT * FROM \"test_02725_1.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test_hdfs_2

#################
echo "Test 2: check exceptions"

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test_hdfs_3;
CREATE DATABASE test_hdfs_3 ENGINE = HDFS('abacaba');
""" 2>&1 | tr '\n' ' ' | grep -oF "BAD_ARGUMENTS"

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test_hdfs_4;
CREATE DATABASE test_hdfs_4 ENGINE = HDFS;
USE test_hdfs_4;
SELECT * FROM \"abacaba/file.tsv\"
""" 2>&1 | tr '\n' ' ' | grep -oF "CANNOT_EXTRACT_TABLE_STRUCTURE"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM test_hdfs_4.\`http://localhost:11111/test/a.tsv\`" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_hdfs_4.\`hdfs://localhost:12222/file.myext\`" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "The data format cannot be detected" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_hdfs_4.\`hdfs://localhost:12222/test_02725_3.tsv\`" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "The table structure cannot be extracted" > /dev/null && echo "OK" || echo 'FAIL' ||:
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_hdfs_4.\`hdfs://localhost:12222\`" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:


# Cleanup
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test_hdfs_1;
DROP DATABASE IF EXISTS test_hdfs_2;
DROP DATABASE IF EXISTS test_hdfs_3;
DROP DATABASE IF EXISTS test_hdfs_4;
"""
