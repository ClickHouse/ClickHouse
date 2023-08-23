#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
[[ -d $dir ]] && rm -rd $dir
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
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('${dir}/tmp.csv')"
echo "implicit:"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM \"${dir}/tmp.csv\""

#################
echo "Test 2: check Filesystem database"
$CLICKHOUSE_LOCAL --multiline --multiquery -q """
DROP DATABASE IF EXISTS test;
CREATE DATABASE test ENGINE = Filesystem('${dir}');
SELECT COUNT(*) FROM test.\`tmp.csv\`;
DROP DATABASE test;
"""

#################
echo "Test 3: check show database with Filesystem"
$CLICKHOUSE_LOCAL --multiline --multiquery -q """
DROP DATABASE IF EXISTS test02707;
CREATE DATABASE test02707 ENGINE = Filesystem('${dir}');
SHOW DATABASES;
DROP DATABASE test02707;
""" | grep "test02707"

# Remove temporary dir with files
rm -rd $dir
