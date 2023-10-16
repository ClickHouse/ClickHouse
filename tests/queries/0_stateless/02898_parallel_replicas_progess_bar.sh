#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r1') ORDER BY k;
CREATE TABLE t2(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r2') ORDER BY k;
CREATE TABLE t3(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r3') ORDER BY k;

insert into t1 select number, number from numbers(1000, 1000);
insert into t2 select number, number from numbers(2000, 1000);
insert into t3 select number, number from numbers(3000, 1000);

system sync replica t1;
system sync replica t2;
system sync replica t3;
"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "SELECT count(), min(k), max(k), avg(k) FROM t1 SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0, use_hedged_requests=0, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost'" -v |& grep -e "X-ClickHouse-Progress:" | sed 's/,\"elapsed_ns[^}]*//' | grep -F -m 1 '"total_rows_to_read":"3000"' | sed 's/\"read_rows[^,]*//' | sed 's/,\"read_bytes[^,]*//'

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;"
