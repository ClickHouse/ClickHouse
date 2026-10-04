#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Numeric keys keep their source width in staged records. Compare counts, fixed and variable
# arguments, and grouping without aggregates across the in-memory and external merge paths.
for spill in 0 1000000
do
    for key in 'toUInt32(number * 1048573)' 'toInt32(number * 1048573)' 'toFloat32(number - 40000)'
    do
        for aggregates in '' ', count()' ', sum(number)' ', min(toString(number))' ', count() AS c, sum(number)' \
            ', sum(k), uniqUpTo(25)(k)' ', sum(k), min(toString(number))'
        do
            order=''
            if [[ "$aggregates" == *'AS c'* ]]
            then
                order='ORDER BY c DESC, k LIMIT 100'
            fi
            for adaptive in 0 1
            do
                $CLICKHOUSE_LOCAL --query "
                    SET max_threads = 4, max_block_size = 8191;
                    SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
                    SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
                    SET max_bytes_before_external_group_by = ${spill}, max_bytes_ratio_before_external_group_by = 0;
                    SET enable_adaptive_aggregator = ${adaptive};
                    SELECT sum(cityHash64(*)) FROM
                        (SELECT ${key} AS k ${aggregates} FROM numbers_mt(80123) GROUP BY k ${order});
                " > "${CLICKHOUSE_TMP}/record_keys_${adaptive}.out"
            done
            diff -u "${CLICKHOUSE_TMP}/record_keys_0.out" "${CLICKHOUSE_TMP}/record_keys_1.out"
        done
        echo "${key}, spill=${spill}"
    done
done

# A numeric grouping key can also supply an aggregate argument. Exercise its source width and
# representation with fixed-only and mixed argument records, including nullable keys and arguments.
for key in 'toUInt64(number % 5000)' 'toUInt128(number % 5000)' 'toUInt256(number % 5000)' \
    'toDecimal128(number % 5000, 3)' 'toLowCardinality(toUInt32(number % 5000))' \
    'if(number % 3 = 0, NULL, toUInt32(number % 5000))'
do
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            SET max_threads = 4, max_block_size = 8191;
            SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
            SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
            SET enable_adaptive_aggregator = ${adaptive};
            SELECT sum(cityHash64(*)) FROM
                (SELECT ${key} AS k, sum(k), uniqUpTo(25)(k), min(toString(number))
                 FROM numbers_mt(80123) GROUP BY k);
        " > "${CLICKHOUSE_TMP}/record_key_argument_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/record_key_argument_0.out" "${CLICKHOUSE_TMP}/record_key_argument_1.out"
    echo "${key} as argument"
done
