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
# `mx` gets a Parquet file with `column_sizes` and an Avro file without; `pr` writes its first file with `k Int32`.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE f (dense Int64, sparse Int64, dc Decimal(18, 2), s String) ENGINE = IcebergLocal('${LAKE}/f');
    INSERT INTO f SELECT number % 2500, (number % 2500) * 1000003, number % 2500, toString(number % 2500)
        FROM numbers(100000);
    CREATE TABLE fp (r Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/fp') PARTITION BY (r);
    INSERT INTO fp SELECT [0, 1000, 2000, 3000, 1000000][number % 5 + 1], number FROM numbers(100000);
    CREATE TABLE fd (d Decimal(9, 2), v Int64) ENGINE = IcebergLocal('${LAKE}/fd') PARTITION BY (d);
    INSERT INTO fd SELECT [1.50, 2.25, 3.00][number % 3 + 1], number FROM numbers(30000);
    CREATE TABLE mx (k Int64, f Float64) ENGINE = IcebergLocal('${LAKE}/mx');
    INSERT INTO mx SELECT number, number FROM numbers(1000);
    INSERT INTO FUNCTION icebergLocal('${LAKE}/mx', 'Avro') SELECT toInt64(number + 1000) AS k, toFloat64(number) AS f
        FROM numbers(1000);
    CREATE TABLE pr (k Int32) ENGINE = IcebergLocal('${LAKE}/pr');
    INSERT INTO pr SELECT number % 500 FROM numbers(1000);
    ALTER TABLE pr MODIFY COLUMN k Int64;
    INSERT INTO pr SELECT number % 500 + 500 FROM numbers(1000);
    ALTER TABLE pr RENAME COLUMN k TO kk;
    CREATE TABLE dim10 (k Int64, kd Decimal(18, 2), ks String, kf Float64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number, number, toString(number), number FROM numbers(10);
    CREATE TABLE dim1 (k Int32, kd Decimal(9, 2)) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim1 SELECT 0, 2.25;
"

echo '--- fixture: data files and rows per Iceberg table; format and column_sizes of f in the files of mx'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"
${CLICKHOUSE_CLIENT} --query "
    SELECT arraySort(groupArray((upper(file_format), mapContains(column_sizes, 2)))) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'mx'"

# Expect 20000: 100000 rows over 5 partition values, so 20000 rows have r = 0, the one key of dim1.
echo '--- T1: rule 1, identity partition: 5 values'
labels "SELECT count() FROM fp JOIN dim1 AS d ON fp.r = d.k" ${ON}
# Expect ~~20000: the 2 remaining partitions hold 40000 rows, so 40000 * 1 / 2; a filter makes it imprecise.
echo '--- T2: rule 1 after partition pruning: 2 values in the remaining files'
labels "SELECT count() FROM fp JOIN dim1 AS d ON fp.r = d.k WHERE fp.r IN (0, 1000)" ${ON}
# Expect 10000: 30000 rows over 3 partition values, so 10000 rows have d = 2.25, the one key of dim1.
echo '--- T3: rule 1, decimal partition: 3 values'
labels "SELECT count() FROM fd JOIN dim1 AS d ON fd.d = d.kd" ${ON}

T4="SELECT count() FROM f JOIN dim10 AS d ON f.dense = d.k"
# Expect 400: dense is 0..2499, NDV 2500, so 100000 * 10 / 2500; each of the 10 keys matches 40 rows.
echo '--- T4: rule 2, dense integer: range 2500'
labels "${T4}" ${ON}
# Expect 10: without column statistics the rows stand in for the NDV, so 100000 * 10 / 100000.
echo '--- T5: use_iceberg_manifest_column_statistics = 0'
labels "${T4}" ${NO_COLUMN_STATS}
# Expect ~~10: as T5, and dim10 has no statistics either, so its 10 rows are imprecise.
echo '--- T6: use_statistics = 0'
labels "${T4}" ${NO_STATS}

# Expect 10: the range is far above the rows, so the NDV is the 100000 rows (2500 true): 100000 * 10 / 100000.
echo '--- T7: rule 2, sparse integer: range clamped to the rows'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.sparse = d.k" ${ON}

# Expect 100000 * 10 / (column_sizes of dc / 8); the size depends on the Parquet encoder, so it is read from the table.
echo '--- T8: rule 3, Decimal(18, 2): ResultRows from column_sizes / 8'
EXPECTED=$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt64(1 / greatest(least(intDiv(sum(column_sizes[3]), 8), 100000), 10) * 100000 * 10)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'f'")
ACTUAL=$(labels "SELECT count() FROM f JOIN dim10 AS d ON f.dc = d.kd" ${ON} | grep 'ResultRows')
if [ "${ACTUAL}" = "ResultRows: ${EXPECTED}" ]; then echo "as expected"; else echo "${ACTUAL}, expected ${EXPECTED}"; fi

# Expect 100: the guess is 10% of the 100000 rows (2500 true), as for MergeTree without `uniq`: 100000 * 10 / 10000.
echo '--- T9: rule 4, String: 10% of the rows'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.s = d.ks" ${ON}
# Expect 100: the Avro file has no column_sizes and Float64 no bounds, so 10% of 2000 rows: 2000 * 10 / 200.
echo '--- T10: no column_sizes in the Avro file and no bounds for Float64: rule 4, 10% of the rows'
labels "SELECT count() FROM mx AS t JOIN dim10 AS d ON t.f = d.kf" ${ON}
# Expect 20: k is 0..499 written as Int32, then 500..999, range 1000: 2000 * 10 / 1000 (40 if bounds were lost).
echo '--- T11: k Int32, then Int64, then renamed to kk: range 1000'
labels "SELECT count() FROM pr AS t JOIN dim10 AS d ON t.kk = d.k" ${ON}

rm -rf "${LAKE}"
