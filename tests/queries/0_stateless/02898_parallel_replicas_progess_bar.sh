#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1(k UInt32, v String) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r1') ORDER BY k settings index_granularity=10;
CREATE TABLE t2(k UInt32, v String) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r2') ORDER BY k settings index_granularity=10;
CREATE TABLE t3(k UInt32, v String) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r3') ORDER BY k settings index_granularity=10;

insert into t1 select number, toString(number) from numbers(1000, 1000);
insert into t2 select number, toString(number) from numbers(2000, 1000);
insert into t3 select number, toString(number) from numbers(3000, 1000);
"

# default coordinator
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "SELECT count(), min(k), max(k), avg(k) FROM t1 SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, use_hedged_requests=0, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = 0.001" -v |& grep -e "X-ClickHouse-Summary:" | grep -F -m 1 '"total_rows_to_read":"3000"' | awk -F ',' '{print $5}'

# reading in order coordinator
query_with_optimize_read_in_order="SELECT k, sipHash64(v) FROM t1 order by k limit 5 offset 998 SETTINGS optimize_read_in_order=1"
$CLICKHOUSE_CLIENT -q "explain plan actions=1 ${query_with_optimize_read_in_order}, allow_experimental_parallel_reading_from_replicas=0" | grep -F 'ReadType: InOrder' | sed 's/^[ \t]*//'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "${query_with_optimize_read_in_order} , allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, use_hedged_requests=0, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = 0.001" -v |& grep -e "X-ClickHouse-Summary:" | grep -F -m 1 '"total_rows_to_read":"3000"' | awk -F ',' '{print $5}'

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;"
