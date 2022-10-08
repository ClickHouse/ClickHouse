#!/usr/bin/env bash
# Tags: no-fasttest, no-s3-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

filename=$CLICKHOUSE_TEST_UNIQUE_NAME
table_function="s3(s3, filename = '$filename')"

clickhouse client --multiquery --multiline  --query "
system drop filesystem cache;
insert into table function $table_function select number from numbers(100) settings s3_truncate_on_insert=1;
"

query="select * from $table_function"

query_id=$(clickhouse client --enable_filesystem_cache_log 1 --enable_cache_for_s3_table_engine 1 --max_download_threads 0 --query "select queryID() from ($query) limit 1" 2>&1)

echo "cold data"
clickhouse client --multiquery --multiline  --query "
system drop filesystem cache;
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='$query_id' order by size;
"

query_id=$(clickhouse client --enable_filesystem_cache_log 1 --enable_cache_for_s3_table_engine 1 --max_download_threads 0 --query "select queryID() from ($query) limit 1" 2>&1)

echo "hot data"
clickhouse client --multiquery --multiline  --query "
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='$query_id' order by size;
"

query_id=$(clickhouse client --enable_filesystem_cache_log 1 --enable_cache_for_s3_table_engine 1 --max_download_threads 1 --query "select queryID() from ($query) limit 1" 2>&1)

echo "cold data"
clickhouse client --multiquery --multiline  --query "
system drop filesystem cache;
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='$query_id' order by size;
"

query_id=$(clickhouse client --enable_filesystem_cache_log 1 --enable_cache_for_s3_table_engine 1 --max_download_threads 1 --query "select queryID() from ($query) limit 1" 2>&1)

echo "hot data"
clickhouse client --multiquery --multiline  --query "
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='$query_id' order by size;
"
