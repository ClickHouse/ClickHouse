#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# See 01658_read_file_to_string_column.sh
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
mkdir -p ${user_files_path}/
cp $CUR_DIR/data_zstd/test_01946.zstd ${user_files_path}/

${CLICKHOUSE_CLIENT} --multiline --multiquery --query "
set min_chunk_bytes_for_parallel_parsing=10485760;
set max_read_buffer_size = 65536;
set input_format_parallel_parsing = 0;
select * from file('test_01946.zstd', 'JSONEachRow', 'foo String') limit 30 format Null;
set input_format_parallel_parsing = 1;
select * from file('test_01946.zstd', 'JSONEachRow', 'foo String') limit 30 format Null;
"

