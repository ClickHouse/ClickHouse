#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prepare data
${CLICKHOUSE_CLIENT} -q "insert into table function hdfs('hdfs://localhost:12222/test_02725_1.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 1, 2, 3 settings hdfs_truncate_on_insert=1;"
${CLICKHOUSE_CLIENT} -q "insert into table function hdfs('hdfs://localhost:12222/test_02725_2.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 4, 5, 6 settings hdfs_truncate_on_insert=1;"

#################
echo "Test 1: select from hdfs database"

# Database without specific host
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
CREATE DATABASE test1 ENGINE = HDFS;
USE test1;
SELECT * FROM \"hdfs://localhost:12222/test_02725_1.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test1

# Database with host
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = HDFS('hdfs://localhost:12222');
USE test2;
SELECT * FROM \"test_02725_1.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test2

#################
echo "Test 2: check exceptions"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test3;
CREATE DATABASE test3 ENGINE = HDFS('abacaba');
""" 2>&1| grep -F "BAD_ARGUMENTS" > /dev/null && echo "OK0"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test4;
CREATE DATABASE test4 ENGINE = HDFS;
USE test4;
SELECT * FROM \"abacaba/file.tsv\"
""" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK1"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM test4.\`http://localhost:11111/test/a.tsv\`" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK2"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test4.\`hdfs://localhost:12222/file.myext\`" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK3"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test4.\`hdfs://localhost:12222/test_02725_3.tsv\`" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK4"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test4.\`hdfs://localhost:12222\`" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK5"


# Cleanup
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
DROP DATABASE IF EXISTS test2;
DROP DATABASE IF EXISTS test3;
DROP DATABASE IF EXISTS test4;
"""
