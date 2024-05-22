#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#  shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')

cp "$CUR_DIR"/data_csv/10m_rows.csv.xz $USER_FILES_PATH/

${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('10m_rows.csv.xz' , 'CSVWithNames') order by identifier, number, name, surname, birthday LIMIT 1 settings input_format_parallel_parsing=1, max_threads=1, max_parsing_threads=16, min_chunk_bytes_for_parallel_parsing=10485760, max_memory_usage=1000000000"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('10m_rows.csv.xz' , 'CSVWithNames') order by identifier, number, name, surname, birthday LIMIT 1 settings input_format_parallel_parsing=1, max_threads=1, max_parsing_threads=16, min_chunk_bytes_for_parallel_parsing=10485760, max_memory_usage=100000000"

rm $USER_FILES_PATH/10m_rows.csv.xz
