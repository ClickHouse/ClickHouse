#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading many CSV files from S3 while memory allocations fail at random must not crash the server.
prefix="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}/f_{_partition_id}.csv', format = 'CSV', structure = 'x UInt64', partition_strategy = 'wildcard') PARTITION BY x SELECT number FROM numbers(100) SETTINGS s3_truncate_on_insert = 1"

query="SELECT x FROM s3(s3_conn, filename = '${prefix}/f_*.csv', format = 'CSV', structure = 'x UInt64') FORMAT Null"
start=$SECONDS
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT --max_threads=1 --input_format_parallel_parsing=0 --max_untracked_memory=0 \
        --memory_tracker_fault_probability=0.001 -q "$query" >/dev/null 2>&1 ||:
    (( SECONDS - start < 60 )) || break
done

$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM s3(s3_conn, filename = '${prefix}/f_*.csv', format = 'CSV', structure = 'x UInt64')"
exit 0
