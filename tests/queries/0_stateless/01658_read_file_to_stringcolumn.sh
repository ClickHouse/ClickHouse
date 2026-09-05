#!/usr/bin/env bash

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A file outside user_files, used to check that reading such a path is rejected
# in client mode and allowed in local mode. Keep it in the per-test unique tmp
# dir instead of a fixed /tmp path so concurrent tests cannot collide on it.
OUTSIDE_FILE="${CLICKHOUSE_TMP}/01658_outside.txt"

# Clean up on EXIT so a mid-script abort (set -e + a failing query) cannot
# leave the short filenames `a`, `b`, `c` in `user_files_path` and break
# other tests that rely on them being absent.
cleanup() {
    rm -f "${CLICKHOUSE_USER_FILES_UNIQUE}"/{a,b,c}.txt
    rm -f "${CLICKHOUSE_USER_FILES_UNIQUE}"/{a,b,c}
    rm -f "${OUTSIDE_FILE}"
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"/dir
}
trap cleanup EXIT

echo -n aaaaaaaaa > ${CLICKHOUSE_USER_FILES_UNIQUE}/a.txt
echo -n bbbbbbbbb > ${CLICKHOUSE_USER_FILES_UNIQUE}/b.txt
echo -n ccccccccc > ${CLICKHOUSE_USER_FILES_UNIQUE}/c.txt
echo -n ccccccccc > "${OUTSIDE_FILE}"
mkdir -p ${CLICKHOUSE_USER_FILES_UNIQUE}/dir


### 1st TEST in CLIENT mode.
${CLICKHOUSE_CLIENT} --query "drop table if exists data;"
${CLICKHOUSE_CLIENT} --query "create table data (A String, B String) engine=MergeTree() order by A;"


# Valid cases:
${CLICKHOUSE_CLIENT} --query "select file('${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "select file('${CLICKHOUSE_TEST_UNIQUE_NAME}/c.txt'), * from data";echo ":"$?
${CLICKHOUSE_CLIENT} --query "
    create table filenames(name String) engine=MergeTree() order by tuple();
    insert into filenames values ('${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt'), ('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt'), ('${CLICKHOUSE_TEST_UNIQUE_NAME}/c.txt');
    select file(name) from filenames format TSV;
    drop table if exists filenames;
"

# Invalid cases: (Here using sub-shell to catch exception avoiding the test quit)
# Test non-exists file
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'nonexist.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt')"'";echo :$?' | bash 2>/dev/null
# Test isDir
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'${CLICKHOUSE_TEST_UNIQUE_NAME}/dir'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt')"'";echo :$?' | bash 2>/dev/null
# Test path out of the user_files directory. It's not allowed in client mode
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'${OUTSIDE_FILE}'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt')"'";echo :$?' | bash 2>/dev/null

# Test relative path consists of ".." whose absolute path is out of the user_files directory.
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'../../../../../../../../../../../../../../../../../../../tmp/c.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt')"'";echo :$?' | bash 2>/dev/null
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'../../../../a.txt'), file('${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt')"'";echo :$?' | bash 2>/dev/null


### 2nd TEST in LOCAL mode.

echo -n aaaaaaaaa > a.txt
echo -n bbbbbbbbb > b.txt
echo -n ccccccccc > c.txt
mkdir -p dir

# Valid cases:
# The default dir is the CWD path in LOCAL mode
${CLICKHOUSE_LOCAL} --query "
    drop table if exists data;
    create table data (A String, B String) engine=MergeTree() order by A;
    select file('a.txt'), file('b.txt');
    insert into data select file('a.txt'), file('b.txt');
    insert into data select file('a.txt'), file('b.txt');
    select file('c.txt'), * from data;
    select file('${OUTSIDE_FILE}'), * from data;
"
echo ":"$?


# Invalid cases: (Here using sub-shell to catch exception avoiding the test quit)
# Test non-exists file
echo "${CLICKHOUSE_LOCAL} --query "'"select file('"'nonexist.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test isDir
echo "${CLICKHOUSE_LOCAL} --query "'"select file('"'dir'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test that the function is not injective

echo -n Hello > ${CLICKHOUSE_USER_FILES_UNIQUE}/a
echo -n Hello > ${CLICKHOUSE_USER_FILES_UNIQUE}/b
echo -n World > ${CLICKHOUSE_USER_FILES_UNIQUE}/c

${CLICKHOUSE_CLIENT} --query "SELECT file(arrayJoin(['${CLICKHOUSE_TEST_UNIQUE_NAME}/a', '${CLICKHOUSE_TEST_UNIQUE_NAME}/b', '${CLICKHOUSE_TEST_UNIQUE_NAME}/c'])) AS s, count() GROUP BY s ORDER BY s"
${CLICKHOUSE_CLIENT} --query "SELECT s, count() FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/?', TSV, 's String') GROUP BY s ORDER BY s"

# Cleanup is handled by the `trap cleanup EXIT` at the top of this script.
