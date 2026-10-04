#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# One group has more than a worker's share of distinct values and the other groups have small states.
# The representations exercise both merge decisions, and the extra aggregates check the state offsets.
for aggregate in \
    'uniqExact(n)' \
    'uniqExact(n % 1000000)' \
    'uniqExact(toString(n))' \
    'uniqExactTuple(tuple(n, toString(n)))' \
    'uniqExactIf(n, n % 2 = 0)' \
    'uniqExact(if(n % 2 = 0, n, NULL))'
do
    echo "$aggregate"
    for suffix in 'ORDER BY k' 'ORDER BY c DESC LIMIT 1'
    do
        query="SELECT k, sum(n) AS s, count() AS c, ${aggregate} AS u
            FROM (SELECT number AS n, if(n % 3 = 0, 0, 1 + n % 100) AS k FROM numbers_mt(4000000))
            GROUP BY k ${suffix}"
        for adaptive in 0 1
        do
            $CLICKHOUSE_LOCAL --query "
                SET max_threads = 4;
                SET max_block_size = 8192;
                SET collect_hash_table_stats_during_aggregation = 0;
                SET adaptive_aggregator_freeze_threshold = 2;
                SET adaptive_aggregator_freeze_threshold_bytes = 0;
                SET adaptive_aggregator_disable_thaw = 1;
                SET group_by_two_level_threshold = 100000;
                SET group_by_two_level_threshold_bytes = 50000000;
                SET max_bytes_before_external_group_by = 0;
                SET max_bytes_ratio_before_external_group_by = 0;
                SET enable_adaptive_aggregator = ${adaptive};
                ${query}" > "${CLICKHOUSE_TMP}/state_merge_work_${adaptive}.out"
        done
        diff -u "${CLICKHOUSE_TMP}/state_merge_work_0.out" "${CLICKHOUSE_TMP}/state_merge_work_1.out"
    done
done
