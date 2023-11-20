#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir $user_files_path/d1
touch $user_files_path/d1/text1.txt

for i in {1..2}
do
	echo $i$'\t'$i >> $user_files_path/d1/text1.txt
done

mkdir $user_files_path/d1/d2
touch $user_files_path/d1/d2/text2.txt
for i in {3..4}
do
	echo $i$'\t'$i >> $user_files_path/d1/d2/text2.txt
done

mkdir $user_files_path/d1/d2/d3
touch $user_files_path/d1/d2/d3/text3.txt
for i in {5..6}
do
	echo $i$'\t'$i >> $user_files_path/d1/d2/d3/text3.txt
done

${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/**','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/*/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort
${CLICKHOUSE_CLIENT} -q "SELECT * from file ('d1/**/tex*','TSV', 'Index UInt8, Number UInt8')" | sort --numeric-sort


rm $user_files_path/d1/d2/d3/text3.txt
rmdir $user_files_path/d1/d2/d3
rm $user_files_path/d1/d2/text2.txt
rmdir $user_files_path/d1/d2
rm $user_files_path/d1/text1.txt
rmdir $user_files_path/d1