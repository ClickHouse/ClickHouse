#!/usr/bin/env bash
# Tags: no-parallel

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -n aaaaaaaaa > ${USER_FILES_PATH}/a.txt
echo -n bbbbbbbbb > ${USER_FILES_PATH}/b.txt
echo -n ccccccccc > ${USER_FILES_PATH}/c.txt
echo -n ccccccccc > /tmp/c.txt
mkdir -p ${USER_FILES_PATH}/dir


### 1st TEST in CLIENT mode.
${CLICKHOUSE_CLIENT} --query "drop table if exists data;"
${CLICKHOUSE_CLIENT} --query "create table data (A String, B String) engine=MergeTree() order by A;"


# Valid cases:
${CLICKHOUSE_CLIENT} --query "select file('a.txt'), file('b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('a.txt'), file('b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('a.txt'), file('b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "select file('c.txt'), * from data";echo ":"$?
${CLICKHOUSE_CLIENT} --query "
    create table filenames(name String) engine=MergeTree() order by tuple();
    insert into filenames values ('a.txt'), ('b.txt'), ('c.txt');
    select file(name) from filenames format TSV;
    drop table if exists filenames;
"

# Invalid cases: (Here using sub-shell to catch exception avoiding the test quit)
# Test non-exists file
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'nonexist.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null
# Test isDir
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'dir'), file('b.txt')"'";echo :$?' | bash 2>/dev/null
# Test path out of the user_files directory. It's not allowed in client mode
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'/tmp/c.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test relative path consists of ".." whose absolute path is out of the user_files directory.
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'../../../../../../../../../../../../../../../../../../../tmp/c.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null
echo "${CLICKHOUSE_CLIENT} --query "'"select file('"'../../../../a.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null


### 2nd TEST in LOCAL mode.

echo -n aaaaaaaaa > a.txt
echo -n bbbbbbbbb > b.txt
echo -n ccccccccc > c.txt
mkdir -p dir
#Test for large files, with length : 699415
c_count=$(wc -c ${CURDIR}/01518_nullable_aggregate_states2.reference | awk '{print $1}')
echo $c_count

# Valid cases:
# The default dir is the CWD path in LOCAL mode
${CLICKHOUSE_LOCAL} --query "
    drop table if exists data;
    create table data (A String, B String) engine=MergeTree() order by A;
    select file('a.txt'), file('b.txt');
    insert into data select file('a.txt'), file('b.txt');
    insert into data select file('a.txt'), file('b.txt');
    select file('c.txt'), * from data;
    select file('/tmp/c.txt'), * from data;
    select $c_count, $c_count -length(file('${CURDIR}/01518_nullable_aggregate_states2.reference'))
"
echo ":"$?


# Invalid cases: (Here using sub-shell to catch exception avoiding the test quit)
# Test non-exists file
echo "${CLICKHOUSE_LOCAL} --query "'"select file('"'nonexist.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test isDir
echo "${CLICKHOUSE_LOCAL} --query "'"select file('"'dir'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test that the function is not injective

echo -n Hello > ${USER_FILES_PATH}/a
echo -n Hello > ${USER_FILES_PATH}/b
echo -n World > ${USER_FILES_PATH}/c

${CLICKHOUSE_CLIENT} --query "SELECT file(arrayJoin(['a', 'b', 'c'])) AS s, count() GROUP BY s ORDER BY s"
${CLICKHOUSE_CLIENT} --query "SELECT s, count() FROM file('?', TSV, 's String') GROUP BY s ORDER BY s"

# Restore
rm ${USER_FILES_PATH}/{a,b,c}.txt
rm ${USER_FILES_PATH}/{a,b,c}
rm /tmp/c.txt
rm -rf ${USER_FILES_PATH}/dir
