#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Repeated group keys receive either fresh values or repeated values. The aggregate representations
# exercise argument sampling for numeric, constant, string, tuple and variadic inputs. Repeating each
# query in one process also checks execution after the aggregation statistics remember its first run.
# Approximate distinct counts and multiple aggregates exercise execution without a distinct-input model.
for aggregate in \
    'uniqExact(n)' \
    'uniqExact(n % 40000)' \
    'uniqExact(7)' \
    'uniqExact(toString(n))' \
    'uniqExact(toString(n % 40000))' \
    'uniqExact(tuple(n, toString(n)))' \
    'uniqExact(n, toString(n))' \
    'uniqHLL12(n)' \
    'uniqExact(n), count()'
do
    echo "$aggregate"
    query="SELECT n % 20000 AS k, ${aggregate}
        FROM (SELECT number AS n FROM numbers_mt(80000)) GROUP BY k ORDER BY k"
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            SET max_threads = 4, max_block_size = 8192;
            SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
            SET adaptive_aggregator_disable_thaw = 0, collect_hash_table_stats_during_aggregation = 1;
            SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
            SET enable_adaptive_aggregator = ${adaptive};
            ${query}; ${query};" > "${CLICKHOUSE_TMP}/distinct_payload_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/distinct_payload_0.out" "${CLICKHOUSE_TMP}/distinct_payload_1.out"
done
