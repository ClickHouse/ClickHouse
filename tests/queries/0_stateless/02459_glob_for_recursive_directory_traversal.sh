#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir $USER_FILES_PATH/d1
touch $USER_FILES_PATH/d1/text1.txt

for i in {1..2}
do
	echo $i$'\t'$i >> $USER_FILES_PATH/d1/text1.txt
done

mkdir $USER_FILES_PATH/d1/d2
touch $USER_FILES_PATH/d1/d2/text2.txt
for i in {3..4}
do
	echo $i$'\t'$i >> $USER_FILES_PATH/d1/d2/text2.txt
done

mkdir $USER_FILES_PATH/d1/d2/d3
touch $USER_FILES_PATH/d1/d2/d3/text3.txt
for i in {5..6}
do
	echo $i$'\t'$i >> $USER_FILES_PATH/d1/d2/d3/text3.txt
done

${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/**','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/*/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/**/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort


rm $USER_FILES_PATH/d1/d2/d3/text3.txt
rmdir $USER_FILES_PATH/d1/d2/d3
rm $USER_FILES_PATH/d1/d2/text2.txt
rmdir $USER_FILES_PATH/d1/d2
rm $USER_FILES_PATH/d1/text1.txt
rmdir $USER_FILES_PATH/d1
