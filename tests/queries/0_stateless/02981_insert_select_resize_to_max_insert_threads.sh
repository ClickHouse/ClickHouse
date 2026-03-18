#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} -q """
    CREATE TABLE t1_local
    (
        n UInt64,
    )
    ENGINE = MergeTree
    ORDER BY n;

    CREATE TABLE t3_dist
    (
        n UInt64,
    )
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't1_local', rand());

    CREATE TABLE t4_pr
    (
        n UInt64,
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02981_insert_select', '1')
    ORDER BY n;

    SYSTEM STOP MERGES t1_local;

    INSERT INTO t1_local SELECT * FROM numbers_mt(1e6);
"""

max_insert_threads=9

echo "inserting into a remote table from local with concurrency equal to max_insert_threads"
${CLICKHOUSE_CLIENT} --max_insert_threads "$max_insert_threads" -q """
    EXPLAIN PIPELINE
    INSERT INTO t3_dist
    SELECT * FROM t1_local;
""" | grep -v EmptySink | grep -c Sink

echo "inserting into a remote table from remote with concurrency max_insert_threads"
${CLICKHOUSE_CLIENT} --max_insert_threads "$max_insert_threads" --parallel_distributed_insert_select 0 -q """
    EXPLAIN PIPELINE
    INSERT INTO t3_dist
    SELECT * FROM t3_dist;
""" | grep -v EmptySink | grep -c Sink

echo "inserting into a remote table from remote (reading with parallel replicas) with concurrency max_insert_threads"
${CLICKHOUSE_CLIENT} --max_insert_threads "$max_insert_threads" --enable_parallel_replicas 2 --cluster_for_parallel_replicas 'parallel_replicas' --max_parallel_replicas 3 -q """
    EXPLAIN PIPELINE
    INSERT INTO t3_dist
    SELECT * FROM t4_pr;
""" | grep -v EmptySink | grep -c Sink
