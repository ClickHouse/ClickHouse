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

# Remove temporary dir with files
rm -rd $dir
