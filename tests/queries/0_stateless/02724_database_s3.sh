#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Depends on AWS

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

#################
echo "Test 1: select from s3"
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test1;
CREATE DATABASE test1 ENGINE = S3;
USE test1;
SELECT * FROM \"http://localhost:11111/test/a.tsv\"
"""
${CLICKHOUSE_CLIENT} -q "SHOW DATABASES;" | grep test1

# check credentials with absolute path
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test2;
CREATE DATABASE test2 ENGINE = S3('', 'test', 'testtest');
USE test2;
SELECT * FROM \"http://localhost:11111/test/b.tsv\"
"""

# check credentials with relative path
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test4;
CREATE DATABASE test4 ENGINE = S3('http://localhost:11111/test', 'test', 'testtest');
USE test4;
SELECT * FROM \"b.tsv\"
"""

# Check named collection loading
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test5;
CREATE DATABASE test5 ENGINE = S3(s3_conn_db);
SELECT * FROM test5.\`b.tsv\`
"""

#################
echo "Test 2: check exceptions"
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test3;
CREATE DATABASE test3 ENGINE = S3;
USE test3;
SELECT * FROM \"http://localhost:11111/test/a.myext\"
""" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "S3_ERROR" > /dev/null && echo "OK" || echo 'FAIL' ||:

${CLICKHOUSE_CLIENT} --multiline -q """
USE test3;
SELECT * FROM \"abacaba\"
""" 2>&1 | tr '\n' ' ' | grep -oF -e "UNKNOWN_TABLE" -e "BAD_ARGUMENTS" > /dev/null && echo "OK" || echo 'FAIL' ||:

# Cleanup
${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS test1;
DROP DATABASE IF EXISTS test2;
DROP DATABASE IF EXISTS test3;
DROP DATABASE IF EXISTS test4;
DROP DATABASE IF EXISTS test5;
"""
