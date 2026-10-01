#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
[[ -d $dir ]] && rm -rd $dir
mkdir $dir
mkdir $dir/nested
mkdir $dir/nested/nested

# Create temporary csv file for tests
echo '"id","str","int","text"' > $dir/tmp.csv
echo '1,"abc",123,"abacaba"' >> $dir/tmp.csv
echo '2,"def",456,"bacabaa"' >> $dir/tmp.csv
echo '3,"story",78912,"acabaab"' >> $dir/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> $dir/tmp.csv

$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_1.jsonl') select * from numbers(1, 10)"
$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_2.jsonl') select * from numbers(11, 10)"

$CLICKHOUSE_LOCAL -q "insert into function file('$dir/tmp_numbers_30.jsonl') select * from numbers(21, 10)"

$CLICKHOUSE_LOCAL -q "insert into function file('$dir/nested/nested_numbers.jsonl') select * from numbers(1)"
$CLICKHOUSE_LOCAL -q "insert into function file('$dir/nested/nested/nested_nested_numbers.jsonl') select * from numbers(1)"

#################
echo "Test 1: check double quotes"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \"${dir}/tmp.csv\""
#################
echo "Test 1a: check double quotes no parsing overflow"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \"${dir}/tmp.csv\"\"bad\"" 2>&1 | grep -c "UNKNOWN_TABLE"
#################
echo "Test 1b: check double quotes empty"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \"\"" 2>&1 | grep -c "SYNTAX_ERROR"
#################
echo "Test 2: check back quotes"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \`${dir}/tmp.csv\`"
#################
echo "Test 2a: check back quotes no parsing overflow"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \`${dir}/tmp.csv\`\`bad\`" 2>&1 | grep -c "UNKNOWN_TABLE"
#################
echo "Test 2b: check back quotes empty"

$CLICKHOUSE_LOCAL -q "SELECT * FROM \`\`" 2>&1 | grep -c "SYNTAX_ERROR"
#################
echo "Test 3: check literal"

$CLICKHOUSE_LOCAL -q "SELECT * FROM '${dir}/tmp.csv'"
#################
echo "Test 3a: check literal no parsing overflow"

$CLICKHOUSE_LOCAL -q "SELECT * FROM '${dir}/tmp.csv''bad'" 2>&1 | grep -c "SYNTAX_ERROR"
#################
echo "Test 3b: check literal empty"

$CLICKHOUSE_LOCAL -q "SELECT * FROM ''" 2>&1 | grep -c "SYNTAX_ERROR"

echo "Test 4: select using * wildcard"
# Extension is required for auto table structure detection
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers_*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/**.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/**********************.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*_numbers_*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*_nu*ers_*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*_nu*ers_2.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*tmp_numbers_*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*tmp_numbers_1*.jsonl'"

echo "Test 4b: select using ? wildcard"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers_?.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers_??.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/??p_numbers??.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_n?mbers_1.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/t?p_n?mbers_?.jsonl'"

echo "Test 4c: select using '{' + '}'  wildcards"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers_{1..3}.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers_{1,2}.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/tmp_numbers__{1,2}.jsonl'" 2>&1 | grep -c "CANNOT_EXTRACT_TABLE_STRUCTURE"

echo "Test 4d: select using ? and * wildcards"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/?*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/?*????.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/?*?***_.jsonl'" 2>&1 | grep -c "CANNOT_EXTRACT_TABLE_STRUCTURE"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/?*????_*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?*_num*e?s_*.jsonl'"

echo "Test 4e: select using ?, * and '{' + '}' wildcards"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?{1,3}.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?{1..3}.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?*_num*e?s_{1..3}.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/*?*_num*e?s_{1,2}.jsonl'"

echo "Test 4f: recursive search"
# /**/* pattern does not look in current directory
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/**/*.jsonl'"
$CLICKHOUSE_LOCAL -q "SELECT count(*) FROM '$dir/nested/**/*.jsonl'"


# Remove temporary dir with files
rm -rd $dir
