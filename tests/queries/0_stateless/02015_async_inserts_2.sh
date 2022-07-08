#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&max_insert_threads=0&group_by_two_level_threshold=100000&group_by_two_level_threshold_bytes=50000000&distributed_aggregation_memory_efficient=1&fsync_metadata=1&priority=1&output_format_parallel_formatting=0&input_format_parallel_parsing=0&min_chunk_bytes_for_parallel_parsing=4031398&max_read_buffer_size=554729&prefer_localhost_replica=0&max_block_size=51672&max_threads=20"

${CLICKHOUSE_CLIENT} --max_insert_threads=0 --group_by_two_level_threshold=100000 --group_by_two_level_threshold_bytes=50000000 --distributed_aggregation_memory_efficient=1 --fsync_metadata=1 --priority=1 --output_format_parallel_formatting=0 --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=4031398 --max_read_buffer_size=554729 --prefer_localhost_replica=0 --max_block_size=51672 --max_threads=20 -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} --max_insert_threads=0 --group_by_two_level_threshold=100000 --group_by_two_level_threshold_bytes=50000000 --distributed_aggregation_memory_efficient=1 --fsync_metadata=1 --priority=1 --output_format_parallel_formatting=0 --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=4031398 --max_read_buffer_size=554729 --prefer_localhost_replica=0 --max_block_size=51672 --max_threads=20 -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
1,"a"
2,"b"' &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
qqqqqqqqqqq' 2>&1 | grep -o "Code: 27" &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
4,"c"
3,"d"' &

wait

${CLICKHOUSE_CLIENT} --max_insert_threads=0 --group_by_two_level_threshold=100000 --group_by_two_level_threshold_bytes=50000000 --distributed_aggregation_memory_efficient=1 --fsync_metadata=1 --priority=1 --output_format_parallel_formatting=0 --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=4031398 --max_read_buffer_size=554729 --prefer_localhost_replica=0 --max_block_size=51672 --max_threads=20 -q "SELECT * FROM async_inserts ORDER BY id"
${CLICKHOUSE_CLIENT} --max_insert_threads=0 --group_by_two_level_threshold=100000 --group_by_two_level_threshold_bytes=50000000 --distributed_aggregation_memory_efficient=1 --fsync_metadata=1 --priority=1 --output_format_parallel_formatting=0 --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=4031398 --max_read_buffer_size=554729 --prefer_localhost_replica=0 --max_block_size=51672 --max_threads=20 -q "SELECT name, rows, level FROM system.parts WHERE table = 'async_inserts' AND database = '$CLICKHOUSE_DATABASE' ORDER BY name"

${CLICKHOUSE_CLIENT} --max_insert_threads=0 --group_by_two_level_threshold=100000 --group_by_two_level_threshold_bytes=50000000 --distributed_aggregation_memory_efficient=1 --fsync_metadata=1 --priority=1 --output_format_parallel_formatting=0 --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=4031398 --max_read_buffer_size=554729 --prefer_localhost_replica=0 --max_block_size=51672 --max_threads=20 -q "DROP TABLE async_inserts"
