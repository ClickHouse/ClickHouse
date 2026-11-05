#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Sources can produce fewer aggregation streams than the thread limit. All producers must use the same
# partition layout when staging and merging keys and states, including groups shared by several producers.
settings="SET max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;"

check()
{
    local label=$1 rows=$2 key=$3 aggregates=$4 threads=$5
    local query="SELECT ${key} AS k ${aggregates} FROM numbers_mt(${rows}) GROUP BY k"
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            ${settings}
            SET max_threads = ${threads};
            SET enable_adaptive_aggregator = ${adaptive};
            SELECT sum(cityHash64(*)) FROM (${query});
        " > "${CLICKHOUSE_TMP}/producer_layout_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/producer_layout_0.out" "${CLICKHOUSE_TMP}/producer_layout_1.out"
    echo "$label"
}

for threads in 16 64
do
    for rows in 100000 400000
    do
        check "Keys ${rows}, threads ${threads}" "$rows" 'number % 100000' '' "$threads"
        check "Count ${rows}, threads ${threads}" "$rows" 'number % 100000' ', count()' "$threads"
        check "States ${rows}, threads ${threads}" "$rows" 'number % 100000' \
            ', sum(number), max(number), uniqExact(number % 200000)' "$threads"
    done
done

# String keys include the empty key, and nontrivial states own memory that must be released exactly once.
check 'String keys and states' 400000 \
    "if(number % 100000 = 0, '', toString(number % 100000))" ', uniqExact(toString(number % 200000))' 64
