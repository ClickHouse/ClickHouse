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

# Pin `max_threads`: the server-level `additional_memory_tracking_per_thread`
# speculatively reserves 4 MiB per pipeline-executor thread, so with the default
# (core-count) `max_threads` the recorded `memory_usage` grows by a few MiB per thread
# and crosses the 500 MB assertion on many-core/ASan runners. A small fixed thread count
# bounds both the reservation and the per-thread read buffers while still exercising the
# subcolumn-size calculation this test guards.
query_id_base=$($CLICKHOUSE_CLIENT -q "select rand64()");
$CLICKHOUSE_CLIENT --query_id=${query_id_base}_1 -q "select json.s from test format Null settings max_result_bytes=0, max_threads=4"
$CLICKHOUSE_CLIENT --query_id=${query_id_base}_2 -q "select tuple.s from test format Null settings max_result_bytes=0, max_threads=4"

$CLICKHOUSE_CLIENT -q """
system flush logs query_log;
select memory_usage < 500000000 from system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND query_id like '%${query_id_base}%' and type='QueryFinish' and current_database = currentDatabase();
drop table test;
"""