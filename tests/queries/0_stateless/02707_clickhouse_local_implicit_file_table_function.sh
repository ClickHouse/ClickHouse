#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=02707_clickhouse_local_tmp
[[ -d $dir ]] && rm -r $dir
mkdir $dir
# Create temporary csv file for tests
echo '"id","str","int","text"' > $dir/tmp.csv
echo '1,"abc",123,"abacaba"' >> $dir/tmp.csv
echo '2,"def",456,"bacabaa"' >> $dir/tmp.csv
echo '3,"story",78912,"acabaab"' >> $dir/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> $dir/tmp.csv

#################
echo "Test 1: check explicit and implicit call of the file table function"

echo "explicit:"
$CLICKHOUSE_LOCAL -q 'SELECT COUNT(*) FROM file("02707_clickhouse_local_tmp/tmp.csv")'
echo "implicit:"
$CLICKHOUSE_LOCAL -q 'SELECT COUNT(*) FROM "02707_clickhouse_local_tmp/tmp.csv"'

#################
echo "Test 2: check FileSystem database"
$CLICKHOUSE_LOCAL --multiline --multiquery -q """
DROP DATABASE IF EXISTS test;
CREATE DATABASE test ENGINE = FileSystem('02707_clickhouse_local_tmp');
SELECT COUNT(*) FROM test.\`tmp.csv\`;
DROP DATABASE test;
"""

#################
echo "Test 3: check show database with FileSystem"
$CLICKHOUSE_LOCAL --multiline --multiquery -q """
DROP DATABASE IF EXISTS test02707;
CREATE DATABASE test02707 ENGINE = FileSystem('02707_clickhouse_local_tmp');
SHOW DATABASES;
DROP DATABASE test02707;
""" | grep "test02707"

rm -r $dir