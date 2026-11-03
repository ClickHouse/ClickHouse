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

$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_1.csv') select * from numbers(1, 10)"
$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_2.csv') select * from numbers(11, 10)"
$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_30.csv') select * from numbers(21, 10)"

readonly nested_dir=$dir/nested
[[ -d $nested_dir ]] && rm -rd $nested_dir
mkdir $nested_dir
mkdir $nested_dir/subnested

cp ${dir}/tmp_numbers_1.csv ${nested_dir}/nested_tmp_numbers_1.csv
cp ${dir}/tmp_numbers_1.csv ${nested_dir}/subnested/subnested_tmp_numbers_1.csv

readonly other_nested_dir=$dir/other_nested
[[ -d $other_nested_dir ]] && rm -rd $other_nested_dir
mkdir $other_nested_dir
cp ${dir}/tmp_numbers_1.csv ${other_nested_dir}/tmp_numbers_1.csv

#################
echo "Test 1: check explicit and implicit call of the file table function"

echo "explicit:"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('${dir}/tmp.csv')"
echo "implicit:"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM \"${dir}/tmp.csv\""

#################
echo "Test 2: check Filesystem database"
$CLICKHOUSE_LOCAL --multiline -q """
DROP DATABASE IF EXISTS test;
CREATE DATABASE test ENGINE = Filesystem('${dir}');
SELECT COUNT(*) FROM test.\`tmp.csv\`;
SELECT COUNT(*) FROM test.\`tmp_numbers_*.csv\`;
SELECT COUNT(*) FROM test.\`nested/nested_tmp_numbers_1*.csv\`;
SELECT count(DISTINCT _path) FROM test.\`*.csv\`;
SELECT count(DISTINCT _path) FROM test.\`**/*.csv\`;
SELECT count(DISTINCT _path) FROM test.\`**/*.csv\` WHERE position(_path, '${nested_dir}') > 0;
SELECT count(DISTINCT _path) FROM test.\`**/*.csv\` WHERE position(_path, '${nested_dir}') = 0;
DROP DATABASE test;
"""

#################
echo "Test 3: check show database with Filesystem"
$CLICKHOUSE_LOCAL --multiline -q """
DROP DATABASE IF EXISTS test02707;
CREATE DATABASE test02707 ENGINE = Filesystem('${dir}');
SHOW DATABASES;
DROP DATABASE test02707;
""" | grep "test02707"

# Remove temporary dir with files
rm -rd $dir
