#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the NDV of an Iceberg read with `use_iceberg_manifest_statistics`, by the first rule applying:
# 1. identity partition values, 2. `max - min + 1` of integer bounds, 3. 10% of rows.
# Each join with a MergeTree table of 10 (or 1) keys estimates `rows * rows_dim / max(ndv, ndv_dim)`.
# - T1: rule 1 on an identity partition.
# - T2: rule 1 over the files left after partition pruning.
# - T3: rule 1 on a decimal partition.
# - T4: rule 2 on a dense integer.
# - T5: T4 with `use_statistics = 0`: no estimate.
# - T6: rule 3 on a sparse integer, whose range is above the rows.
# - T7: rule 3 on `String`.
# - T8: bounds written before `MODIFY COLUMN ... Int64` keep their values after the column is renamed.

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
ON="--use_iceberg_manifest_statistics=1 --use_statistics=1"
NO_STATS="--use_iceberg_manifest_statistics=1 --use_statistics=0"

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
# `ice_promoted` writes its first file with `int_key Int32`.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE ice_keys (dense_int Int64, sparse_int Int64, string_key String) ENGINE = IcebergLocal('${LAKE}/ice_keys');
    INSERT INTO ice_keys SELECT number % 2500, (number % 2500) * 1000003, toString(number % 2500) FROM numbers(100000);
    CREATE TABLE ice_partitioned (part_key Int32, value Int64) ENGINE = IcebergLocal('${LAKE}/ice_partitioned')
        PARTITION BY (part_key);
    INSERT INTO ice_partitioned SELECT [0, 1000, 2000, 3000, 1000000][number % 5 + 1], number FROM numbers(100000);
    CREATE TABLE ice_decimal_partitioned (part_key Decimal(9, 2), value Int64)
        ENGINE = IcebergLocal('${LAKE}/ice_decimal_partitioned') PARTITION BY (part_key);
    INSERT INTO ice_decimal_partitioned SELECT [1.50, 2.25, 3.00][number % 3 + 1], number FROM numbers(30000);
    CREATE TABLE ice_promoted (int_key Int32) ENGINE = IcebergLocal('${LAKE}/ice_promoted');
    INSERT INTO ice_promoted SELECT number % 500 FROM numbers(1000);
    ALTER TABLE ice_promoted MODIFY COLUMN int_key Int64;
    INSERT INTO ice_promoted SELECT number % 500 + 500 FROM numbers(1000);
    ALTER TABLE ice_promoted RENAME COLUMN int_key TO renamed_key;
    CREATE TABLE dim10 (int_key Int64, string_key String) ENGINE = MergeTree ORDER BY int_key
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number, toString(number) FROM numbers(10);
    CREATE TABLE dim1 (int_key Int32, decimal_key Decimal(9, 2)) ENGINE = MergeTree ORDER BY int_key
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim1 SELECT 0, 2.25;
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

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
echo '--- T5: use_statistics = 0'
echo 'Expect no estimate for ice_keys and no result rows: the manifests are not read; dim10 rows are imprecise.'
labels "${T4}" ${NO_STATS}

echo '--- T6: rule 3, sparse integer: range above the rows'
echo 'Expect 100: the range is far above the rows, so 10% of them: 100000 * 10 / 10000 (true NDV 2500, result 40).'
labels "SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.sparse_int = dim10.int_key" ${ON}

echo '--- T7: rule 3, String: 10% of the rows'
echo 'Expect 100: 10% of the 100000 rows, as MergeTree without `uniq`: 100000 * 10 / 10000 (true NDV 2500, result 400).'
labels "SELECT count() FROM ice_keys JOIN dim10 ON ice_keys.string_key = dim10.string_key" ${ON}
echo '--- T8: int_key Int32, then Int64, then renamed to renamed_key: range 1000'
echo 'Expect 20: int_key is 0..499 as Int32, then 500..999, range 1000: 2000 * 10 / 1000 (40 if the bounds were lost).'
labels "SELECT count() FROM ice_promoted JOIN dim10 ON ice_promoted.renamed_key = dim10.int_key" ${ON}

rm -rf "${LAKE}"
