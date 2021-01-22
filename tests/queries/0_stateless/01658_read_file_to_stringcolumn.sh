#!/usr/bin/env bash
set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation
# When run with client mode on different machine to the server, the data-file creation maybe implemented in SQL. Now we just make it simple
mkdir -p /var/lib/clickhouse/user_files/
echo -n aaaaaaaaa > /var/lib/clickhouse/user_files/a.txt
echo -n bbbbbbbbb > /var/lib/clickhouse/user_files/b.txt
echo -n ccccccccc > /var/lib/clickhouse/user_files/c.txt
echo -n ccccccccc > /tmp/c.txt
mkdir -p /var/lib/clickhouse/user_files/dir

### 1st TEST in CLIENT mode.
${CLICKHOUSE_CLIENT} --query "drop table if exists data;"
${CLICKHOUSE_CLIENT} --query "create table data (A String, B String) engine=MergeTree() order by A;"


# Valid cases:
${CLICKHOUSE_CLIENT} --query "select file('/var/lib/clickhouse/user_files/a.txt'), file('/var/lib/clickhouse/user_files/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('/var/lib/clickhouse/user_files/a.txt'), file('/var/lib/clickhouse/user_files/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "insert into data select file('/var/lib/clickhouse/user_files/a.txt'), file('/var/lib/clickhouse/user_files/b.txt');";echo ":"$?
${CLICKHOUSE_CLIENT} --query "select file('/var/lib/clickhouse/user_files/c.txt'), * from data";echo ":"$?


# Invalid cases: (Here using sub-shell to catch exception avoiding the test quit)
# Test non-exists file
echo "clickhouse-client --query "'"select file('"'nonexist.txt'), file('/var/lib/clickhouse/user_files/b.txt')"'";echo :$?' | bash 2>/dev/null
# Test isDir
echo "clickhouse-client --query "'"select file('"'/var/lib/clickhouse/user_files/dir'), file('/var/lib/clickhouse/user_files/b.txt')"'";echo :$?' | bash 2>/dev/null
# Test path out of the user_files directory. It's not allowed in client mode
echo "clickhouse-client --query "'"select file('"'/tmp/c.txt'), file('/var/lib/clickhouse/user_files/b.txt')"'";echo :$?' | bash 2>/dev/null

# Test relative path consists of ".." whose absolute path is out of the user_files directory.
echo "clickhouse-client --query "'"select file('"'/var/lib/clickhouse/user_files/../../../../tmp/c.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null
echo "clickhouse-client --query "'"select file('"'../../../../a.txt'), file('/var/lib/clickhouse/user_files/b.txt')"'";echo :$?' | bash 2>/dev/null



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
echo "clickhouse-local --query "'"select file('"'nonexist.txt'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Test isDir
echo "clickhouse-local --query "'"select file('"'dir'), file('b.txt')"'";echo :$?' | bash 2>/dev/null

# Restore
rm -rf a.txt b.txt c.txt dir
rm -rf /var/lib/clickhouse/user_files/a.txt
rm -rf /var/lib/clickhouse/user_files/b.txt
rm -rf /var/lib/clickhouse/user_files/c.txt
rm -rf /tmp/c.txt
rm -rf /var/lib/clickhouse/user_files/dir
