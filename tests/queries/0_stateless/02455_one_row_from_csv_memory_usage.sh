#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#  shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_files_path=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')
cp "$CUR_DIR"/data_csv/10m_rows.csv.xz $user_files_path/

${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('$user_files_path/10m_rows.csv.xz' , 'CSVWithNames') LIMIT 1 settings max_memory_usage=1000000000"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('$user_files_path/10m_rows.csv.xz' , 'CSVWithNames') LIMIT 1 settings max_memory_usage=100000000"
