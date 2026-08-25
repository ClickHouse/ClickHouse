#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


mkdir -p ${USER_FILES_PATH}/
cp $CUR_DIR/data_zstd/test_01946.zstd ${USER_FILES_PATH}/

${CLICKHOUSE_CLIENT} --multiline --query "
set min_chunk_bytes_for_parallel_parsing=10485760;
set max_read_buffer_size = 65536;
set input_format_parallel_parsing = 0;
select * from file('test_01946.zstd', 'JSONEachRow', 'foo String') order by foo limit 30 format Null;
set input_format_parallel_parsing = 1;
select * from file('test_01946.zstd', 'JSONEachRow', 'foo String') order by foo limit 30 format Null;
"
