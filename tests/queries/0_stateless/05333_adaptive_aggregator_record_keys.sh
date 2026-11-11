#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Every case runs in a single `clickhouse-local` per aggregator, because process startup
# dominates the run time of these small inputs in sanitizer builds.
common_settings="
    SET max_threads = 4, max_block_size = 1637;
    SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;"

# Runs `queries` with the ordinary and the adaptive aggregator and compares the results.
compare()
{
    local name=$1 queries=$2
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            ${common_settings}
            SET enable_adaptive_aggregator = ${adaptive};
            ${queries}
        " > "${CLICKHOUSE_TMP}/${name}_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/${name}_0.out" "${CLICKHOUSE_TMP}/${name}_1.out"
}

# Numeric keys keep their source width in staged records. Compare counts, fixed and variable
# arguments, and grouping without aggregates across the in-memory and external merge paths.
queries=''
labels=()
for spill in 0 200000
do
    queries+="SET max_bytes_before_external_group_by = ${spill}, max_bytes_ratio_before_external_group_by = 0;"
    for key in 'toUInt32(number * 1048573)' 'toInt32(number * 1048573)' 'toFloat32(number - 8000)'
    do
        label="${key}, spill=${spill}"
        labels+=("${label}")
        queries+="SELECT '${label}';"
        for aggregates in '' ', count()' ', sum(number)' ', min(toString(number))' ', count() AS c, sum(number)' \
            ', sum(k), uniqUpTo(25)(k)' ', sum(k), min(toString(number))'
        do
            order=''
            if [[ "$aggregates" == *'AS c'* ]]
            then
                order='ORDER BY c DESC, k LIMIT 100'
            fi
            queries+="SELECT sum(cityHash64(*)) FROM
                (SELECT ${key} AS k ${aggregates} FROM numbers_mt(16025) GROUP BY k ${order});"
        done
    done
done
compare record_keys "${queries}"
printf '%s\n' "${labels[@]}"

# A numeric grouping key can also supply an aggregate argument. Exercise its source width and
# representation with fixed-only and mixed argument records, including nullable keys and arguments.
queries=''
labels=()
for key in 'toUInt64(number % 5000)' 'toUInt128(number % 5000)' 'toUInt256(number % 5000)' \
    'toDecimal128(number % 5000, 3)' 'toLowCardinality(toUInt32(number % 5000))' \
    'if(number % 3 = 0, NULL, toUInt32(number % 5000))'
do
    label="${key} as argument"
    labels+=("${label}")
    queries+="SELECT '${label}';
        SELECT sum(cityHash64(*)) FROM
            (SELECT ${key} AS k, sum(k), uniqUpTo(25)(k), min(toString(number))
             FROM numbers_mt(16025) GROUP BY k);"
done
compare record_key_argument "${queries}"
printf '%s\n' "${labels[@]}"
