#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir $CLICKHOUSE_USER_FILES_UNIQUE/d1
touch $CLICKHOUSE_USER_FILES_UNIQUE/d1/text1.txt

for i in {1..2}
do
	echo $i$'\t'$i >> $CLICKHOUSE_USER_FILES_UNIQUE/d1/text1.txt
done

mkdir $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2
touch $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/text2.txt
for i in {3..4}
do
	echo $i$'\t'$i >> $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/text2.txt
done

mkdir $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/d3
touch $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/d3/text3.txt
for i in {5..6}
do
	echo $i$'\t'$i >> $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/d3/text3.txt
done

${CLICKHOUSE_CLIENT} -q "SELECT * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/d1/*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/d1/**','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/d1/*/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/d1/**/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort


rm $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/d3/text3.txt
rmdir $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/d3
rm $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2/text2.txt
rmdir $CLICKHOUSE_USER_FILES_UNIQUE/d1/d2
rm $CLICKHOUSE_USER_FILES_UNIQUE/d1/text1.txt
rmdir $CLICKHOUSE_USER_FILES_UNIQUE/d1
