#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the NDV of an Iceberg read with `use_iceberg_manifest_column_statistics`, by the first rule applying:
# 1. identity partition values, 2. `max - min + 1` of integer bounds, 3. `column_sizes` / type width, 4. 10% of rows.
# Each join with a MergeTree table of 10 (or 1) keys estimates `rows * rows_dim / max(ndv, ndv_dim)`.
# - T1: rule 1 on an identity partition.
# - T2: rule 1 over the files left after partition pruning.
# - T3: rule 1 on a decimal partition.
# - T4: rule 2 on a dense integer.
# - T5: T4 with `use_iceberg_manifest_column_statistics = 0`: rows only.
# - T6: T4 with `use_statistics = 0`: rows only.
# - T7: rule 2 on a sparse integer, clamped to the rows.
# - T8: rule 3 on `Decimal`, which has bounds but no range rule.
# - T9: rule 4 on `String`.
# - T10: a file without `column_sizes` skips rule 3.
# - T11: bounds written before `MODIFY COLUMN ... Int64` keep their values after the column is renamed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
# `use_statistics` is passed per call, since a client flag cannot be repeated.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=1"
NO_STATS="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=0"
NO_COLUMN_STATS="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=0 --use_statistics=1"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Prints the `Join:` and `ResultRows:` lines of the logical plan. Usage: labels <query> [client flags].
labels()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${query})
        WHERE explain LIKE '%Join: %' OR explain LIKE '%ResultRows: %'"
}

# `uniq` gives the dimensions an exact count and an NDV equal to their distinct values.
# `ice_mixed_formats` gets a Parquet file with `column_sizes` and an Avro file without; `ice_promoted` writes its first
# file with `int_key Int32`.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE ice_keys (dense_int Int64, sparse_int Int64, decimal_key Decimal(18, 2), string_key String)
        ENGINE = IcebergLocal('${LAKE}/ice_keys');
    INSERT INTO ice_keys SELECT number % 2500, (number % 2500) * 1000003, number % 2500, toString(number % 2500)
        FROM numbers(100000);
    CREATE TABLE ice_partitioned (part_key Int32, value Int64) ENGINE = IcebergLocal('${LAKE}/ice_partitioned')
        PARTITION BY (part_key);
    INSERT INTO ice_partitioned SELECT [0, 1000, 2000, 3000, 1000000][number % 5 + 1], number FROM numbers(100000);
    CREATE TABLE ice_decimal_partitioned (part_key Decimal(9, 2), value Int64)
        ENGINE = IcebergLocal('${LAKE}/ice_decimal_partitioned') PARTITION BY (part_key);
    INSERT INTO ice_decimal_partitioned SELECT [1.50, 2.25, 3.00][number % 3 + 1], number FROM numbers(30000);
    CREATE TABLE ice_mixed_formats (id Int64, float_key Float64) ENGINE = IcebergLocal('${LAKE}/ice_mixed_formats');
    INSERT INTO ice_mixed_formats SELECT number, number FROM numbers(1000);
    INSERT INTO FUNCTION icebergLocal('${LAKE}/ice_mixed_formats', 'Avro')
        SELECT toInt64(number + 1000) AS id, toFloat64(number) AS float_key FROM numbers(1000);
    CREATE TABLE ice_promoted (int_key Int32) ENGINE = IcebergLocal('${LAKE}/ice_promoted');
    INSERT INTO ice_promoted SELECT number % 500 FROM numbers(1000);
    ALTER TABLE ice_promoted MODIFY COLUMN int_key Int64;
    INSERT INTO ice_promoted SELECT number % 500 + 500 FROM numbers(1000);
    ALTER TABLE ice_promoted RENAME COLUMN int_key TO renamed_key;
    CREATE TABLE dim10 (int_key Int64, decimal_key Decimal(18, 2), string_key String, float_key Float64)
        ENGINE = MergeTree ORDER BY int_key SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number, number, toString(number), number FROM numbers(10);
    CREATE TABLE dim1 (int_key Int32, decimal_key Decimal(9, 2)) ENGINE = MergeTree ORDER BY int_key
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim1 SELECT 0, 2.25;
"

echo '--- fixture: data files and rows per Iceberg table; format and float_key column_sizes of ice_mixed_formats'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"
${CLICKHOUSE_CLIENT} --query "
    SELECT arraySort(groupArray((upper(file_format), mapContains(column_sizes, 2)))) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'ice_mixed_formats'"

echo '--- T1: rule 1, identity partition: 5 values'
echo 'Expect 20000: 100000 rows over 5 partition values, so 20000 rows have part_key = 0, the one key of dim1.'
labels "SELECT count() FROM ice_partitioned JOIN dim1 ON ice_partitioned.part_key = dim1.int_key" ${ON}
echo '--- T2: rule 1 after partition pruning: 2 values in the remaining files'
echo 'Expect ~~20000: 2 remaining partitions hold 40000 rows, so 40000 * 1 / max(2, 1); a filter makes it imprecise.'
labels "SELECT count() FROM ice_partitioned JOIN dim1 ON ice_partitioned.part_key = dim1.int_key
    WHERE ice_partitioned.part_key IN (0, 1000)" ${ON}
echo '--- T3: rule 1, decimal partition: 3 values'
echo 'Expect 10000: 30000 rows over 3 partition values, so 10000 rows have part_key = 2.25, the one key of dim1.'
labels "SELECT count() FROM ice_decimal_partitioned
    JOIN dim1 ON ice_decimal_partitioned.part_key = dim1.decimal_key" ${ON}

T4="SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.dense_int = dim10.int_key"
echo '--- T4: rule 2, dense integer: range 2500'
echo 'Expect 400: dense_int is 0..2499, NDV 2500, so 100000 * 10 / 2500; each of the 10 keys matches 40 rows.'
labels "${T4}" ${ON}
echo '--- T5: use_iceberg_manifest_column_statistics = 0'
echo 'Expect 10: without column statistics the rows stand in for the NDV, so 100000 * 10 / 100000.'
labels "${T4}" ${NO_COLUMN_STATS}
echo '--- T6: use_statistics = 0'
echo 'Expect ~~10: as T5, and dim10 has no statistics either, so its 10 rows are imprecise.'
labels "${T4}" ${NO_STATS}

echo '--- T7: rule 2, sparse integer: range clamped to the rows'
echo 'Expect 10: the range is far above the rows, so NDV = 100000: 100000 * 10 / 100000 (true NDV 2500, result 40).'
labels "SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.sparse_int = dim10.int_key" ${ON}

echo '--- T8: rule 3, Decimal(18, 2): ResultRows from column_sizes / 8'
echo 'Expect 100000 * 10 / (column_sizes of decimal_key / 8); the size depends on the Parquet encoder, so it is read.'
EXPECTED=$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt64(1 / greatest(least(intDiv(sum(column_sizes[3]), 8), 100000), 10) * 100000 * 10)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'ice_keys'")
ACTUAL=$(labels "SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.decimal_key = dim10.decimal_key" ${ON} \
    | grep 'ResultRows')
if [ "${ACTUAL}" = "ResultRows: ${EXPECTED}" ]; then echo "as expected"; else echo "${ACTUAL}, expected ${EXPECTED}"; fi

echo '--- T9: rule 4, String: 10% of the rows'
echo 'Expect 100: 10% of the 100000 rows, as MergeTree without `uniq`: 100000 * 10 / 10000 (true NDV 2500, result 400).'
labels "SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.string_key = dim10.string_key" ${ON}
echo '--- T10: Float64: no bounds written, not countable, no column_sizes in the Avro file: rule 4, 10% of rows'
echo 'Expect 100: rule 2 skips floats, rule 3 needs column_sizes in every file: 10% of 2000 rows, 2000 * 10 / 200.'
labels "SELECT count() FROM ice_mixed_formats JOIN dim10 ON ice_mixed_formats.float_key = dim10.float_key" ${ON}
echo '--- T11: int_key Int32, then Int64, then renamed to renamed_key: range 1000'
echo 'Expect 20: int_key is 0..499 as Int32, then 500..999, range 1000: 2000 * 10 / 1000 (40 if the bounds were lost).'
labels "SELECT count() FROM ice_promoted JOIN dim10 ON ice_promoted.renamed_key = dim10.int_key" ${ON}

rm -rf "${LAKE}"
