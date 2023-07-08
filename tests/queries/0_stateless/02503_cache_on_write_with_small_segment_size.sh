#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-s3-storage, no-random-settings

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function random {
     cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z' | fold -w ${1:-8} | head -n 1
}

${CLICKHOUSE_CLIENT} --multiline --multiquery -q "
drop table if exists ttt;
create table ttt (id Int32, value String) engine=MergeTree() order by tuple()  settings storage_policy='s3_cache_small_segment_size', min_bytes_for_wide_part=0;
insert into ttt select number, toString(number) from numbers(100000) settings throw_on_error_from_cache_on_write_operations = 1;
"

query_id=$(random 8)

${CLICKHOUSE_CLIENT} --query_id "$query_id" -q "
select * from ttt format Null settings enable_filesystem_cache_log=1;
"
${CLICKHOUSE_CLIENT} --query_id "$query_id" -q " system flush logs"

${CLICKHOUSE_CLIENT}  -q "
select count() from system.filesystem_cache_log where query_id = '$query_id' AND read_type != 'READ_FROM_CACHE';
"
${CLICKHOUSE_CLIENT}  -q "
select count() from system.filesystem_cache_log where query_id = '$query_id' AND read_type == 'READ_FROM_CACHE';
"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q "
select count() from ttt;
drop table ttt no delay;
"
