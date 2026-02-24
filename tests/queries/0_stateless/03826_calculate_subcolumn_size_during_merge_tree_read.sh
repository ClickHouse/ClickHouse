#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q """
drop table if exists test;
create table test (json JSON, tuple Tuple(s String)) engine=MergeTree settings min_bytes_for_wide_part=1;
insert into test select toJSONString(map('s', repeat('a', 1000))), tuple(repeat('a', 1000)) from numbers(1000000);
"""

query_id_base=$($CLICKHOUSE_CLIENT -q "select rand64()");
$CLICKHOUSE_CLIENT --query_id=${query_id_base}_1 -q "select json.s from test format Null settings max_result_bytes=0"
$CLICKHOUSE_CLIENT --query_id=${query_id_base}_2 -q "select tuple.s from test format Null settings max_result_bytes=0"

$CLICKHOUSE_CLIENT -q """
system flush logs query_log;
select memory_usage < 500000000 from system.query_log where query_id like '%${query_id_base}%' and type='QueryFinish' and current_database = currentDatabase();
drop table test;
"""