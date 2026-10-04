#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Fixed-width arguments cover every specialized copy size, wider fields, null maps, constants and
# low-cardinality normalization. Block boundaries leave partial staging batches. Variable-width keys
# and arguments exercise the same fixed-field packing within records that also contain serialized data.
aggregates="uniqUpTo(25)(number), sum(toUInt128(number)), sum(toDecimal256(number, 3)),
    min(toLowCardinality(toFixedString(char(65 + number % 26), 3))), min(7),
    min(toNullable(toUInt128(23))), max(toString(number)), max([number]),
    sequenceMatch('(?1)(?t<10000)(?2)')(toDateTime(number), number % 3 = 0, number % 5 = 0)"
for width in {1..32} 33 65
do
    value="toFixedString(char(65 + number % 26), ${width})"
    aggregates+=", min(${value}), min(if(number % 3 = 0, NULL, ${value}))"
done

check()
{
    local label=$1 key=$2 spill=$3
    local query="SELECT ${key} AS k, ${aggregates} FROM numbers_mt(80123) GROUP BY k"
    for adaptive in 0 1
    do
        $CLICKHOUSE_LOCAL --query "
            SET max_threads = 4, max_block_size = 8191, optimize_injective_functions_in_group_by = 0;
            SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
            SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
            SET max_bytes_before_external_group_by = ${spill}, max_bytes_ratio_before_external_group_by = 0;
            SET enable_adaptive_aggregator = ${adaptive};
            SELECT sum(cityHash64(*)) FROM (${query});
        " > "${CLICKHOUSE_TMP}/argument_gather_${adaptive}.out"
    done
    diff -u "${CLICKHOUSE_TMP}/argument_gather_0.out" "${CLICKHOUSE_TMP}/argument_gather_1.out"
    echo "$label"
}

check 'Numeric key' 'number % 5000' 0
check 'Packed keys' 'tuple(number % 5000, toUInt32(number % 7))' 0
check 'String key' "concat('key_', toString(number % 5000))" 0
check 'Serialized key' '[number % 5000]' 0
check 'Spilled arguments' 'number % 5000' 1000000

# Successive fixed records exercise key and argument copies at different alignments within a chunk.
for width in 4 5 6 7
do
    aggregates="min(toFixedString(char(65 + number % 26), ${width}))"
    check "Fixed record width ${width}" 'number % 5000' 0
done

# Hash joins use the contiguous row-store writer. Compare its output with columnar payloads for the
# same nullable and non-nullable fixed-width fields, including source blocks with partial final batches.
fields='number AS k'
for width in {1..32}
do
    value="toFixedString(char(65 + number % 26), ${width})"
    fields+=", ${value} AS v_${width}, if(number % 3 = 0, NULL, ${value}) AS n_${width}"
done
for row_store in 0 1
do
    $CLICKHOUSE_LOCAL --query "
        SET join_algorithm = 'hash', max_threads = 4, max_block_size = 8191;
        SET query_plan_optimize_join_order_limit = 0, collect_hash_table_stats_during_joins = 0;
        SET min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_row_store = ${row_store};
        SELECT sum(cityHash64(*)) FROM
            (SELECT r.* FROM (SELECT number % 1000 AS k FROM numbers(20000)) AS l
             INNER JOIN (SELECT ${fields} FROM numbers(1000)) AS r ON l.k = r.k);
    " > "${CLICKHOUSE_TMP}/argument_gather_join_${row_store}.out"
done
diff -u "${CLICKHOUSE_TMP}/argument_gather_join_0.out" "${CLICKHOUSE_TMP}/argument_gather_join_1.out"
echo 'Contiguous row store'
