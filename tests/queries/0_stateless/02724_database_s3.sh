#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Depends on AWS

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

#################
echo "Test 1: select from s3"
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
CREATE DATABASE test1 ENGINE = S3;
USE test1;
SELECT * FROM \"http://localhost:11111/test/a.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test1
${CLICKHOUSE_CLIENT} -q "DROP DATABASE test1;"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = S3('test', 'testtest');
USE test2;
SELECT * FROM \"http://localhost:11111/test/b.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "DROP DATABASE test2;"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM \"http://localhost:11111/test/b.tsv\""

#################
echo "Test 2: check exceptions"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM \"http://localhost:11111/test/c.myext\"" 2>&1| grep -F "UNKNOWN_TABLE" > /dev/null && echo "OK"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test3;
CREATE DATABASE test3 ENGINE = S3;
USE test3;
SELECT * FROM \"http://localhost:11111/test/a.myext\"
""" 2>&1| grep -F "BAD_ARGUMENTS" > /dev/null && echo "OK"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
USE test3;
SELECT * FROM \"abacaba\"
""" 2>&1| grep -F "BAD_ARGUMENTS" > /dev/null && echo "OK"

# Cleanup
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP DATABASE IF EXISTS test1;
DROP DATABASE IF EXISTS test2;
DROP DATABASE IF EXISTS test3;
"""
