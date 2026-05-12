#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select _file, _size from url('http://localhost:11111/test/{a,b,c}.tsv', 'One') order by _file"
$CLICKHOUSE_CLIENT -q "select _file, _size from url('http://localhost:11111/test/{a,b,c}.tsv', 'One') order by _file"

$CLICKHOUSE_CLIENT -q "select _file, _size from s3('http://localhost:11111/test/{a,b,c}.tsv', 'One') order by _file"
$CLICKHOUSE_CLIENT -q "select _file, _size from s3('http://localhost:11111/test/{a,b,c}.tsv', 'One') order by _file"

query_id="02922_one_format_s3_$(random_str 10)"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "select count() from s3('http://localhost:11111/test/{a,b,c}.tsv', 'One') settings log_queries=1, remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1, use_cache_for_count_from_files=0"
$CLICKHOUSE_CLIENT -q "system flush logs query_log"
$CLICKHOUSE_CLIENT -q "select ProfileEvents['ObjectStorageReadObjects'], ProfileEvents['EngineFileLikeReadFiles'] from system.query_log where event_date >= yesterday() and event_time >= now() - 600 and current_database = currentDatabase() and query_id = '$query_id' and type = 'QueryFinish' order by event_time_microseconds desc limit 1"
