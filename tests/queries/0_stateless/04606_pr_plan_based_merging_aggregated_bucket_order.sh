#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shard of a `Distributed` table reads its local table with parallel replicas, merges the partially
# aggregated data of the replicas and sends the result to the initiator, which merges it memory
# efficiently. That merge has to produce the two-level buckets in the order of their id-s, otherwise
# the initiator merges and outputs the same bucket twice and the result contains duplicated keys.
#
# This does not need the buckets to be produced out of order, so the setting is disabled here.
# The buckets of a two level result are converted by a thread pool only for more than 100000 keys,
# and it is the thread pool which returns them in an arbitrary order, hence 200000 keys here.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS d;
    DROP TABLE IF EXISTS t;

    CREATE TABLE t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t SELECT number % 200000, number FROM numbers_mt(400000);
    CREATE TABLE d AS t ENGINE = Distributed('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t);

    SELECT count() - uniqExact(x) AS duplicate_groups FROM (SELECT x, sum(y) FROM d GROUP BY x)
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_plan_based = 1,
        parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 0,
        distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, max_threads = 8,
        enable_producing_buckets_out_of_order_in_aggregation = 0;

    DROP TABLE d;
    DROP TABLE t;
"
