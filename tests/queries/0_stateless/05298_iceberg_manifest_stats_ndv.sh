#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_statistics`, `use_iceberg_manifest_column_statistics` and `use_statistics`
# an Iceberg read also reports the number of distinct values of its columns, by the first rule that applies:
# 1. the identity partition values,
# 2. `max - min + 1` of the value bounds for integers and dates, 3. `column_sizes` over the width of a fixed-width
# type, 4. a guess from the type (`String` half the rows, others 30%), clamped to the rows.
# T11: an equi-join with a MergeTree table of 10 (or 1) distinct keys estimates
# `rows_ice * rows_dim / max(ndv_ice, ndv_dim)` rows, so each arm shows the rule of one column.
# T17: with `use_statistics = 0` or `use_iceberg_manifest_column_statistics = 0` the read reports its rows only.

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
OFF="--use_iceberg_manifest_statistics=0 --use_statistics=1"

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

# `f` is Parquet, which gets `column_sizes`; `fa` is Avro, which gets bounds but no `column_sizes`.
# `uniq` gives the dimensions an exact count and an NDV equal to their rows.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE f (dense Int64, sparse Int64, fl Float64, dc Decimal(18, 2), s String) ENGINE = IcebergLocal('${LAKE}/f');
    INSERT INTO f SELECT number % 2500, (number % 2500) * 1000003, number % 2500, number % 2500, toString(number % 2500)
        FROM numbers(100000);
    CREATE TABLE fa (dense Int64, fl Float64) ENGINE = IcebergLocal('${LAKE}/fa', 'Avro');
    INSERT INTO fa SELECT number % 2500, number % 2500 FROM numbers(100000);
    CREATE TABLE fp (r Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/fp') PARTITION BY (r);
    INSERT INTO fp SELECT [0, 1000, 2000, 3000, 1000000][number % 5 + 1], number FROM numbers(100000);
    CREATE TABLE dim10 (k Int64, kf Float64, kd Decimal(18, 2), ks String, kr Int32) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number, number, number, toString(number), number FROM numbers(10);
    CREATE TABLE dim1 (k Int32) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim1 SELECT 0;
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

echo '--- T11 rule 1, identity partition: 5 values'
labels "SELECT count() FROM fp JOIN dim1 AS d ON fp.r = d.k" ${ON}
echo '--- T11 rule 1 after partition pruning: 2 values in the surviving files'
labels "SELECT count() FROM fp JOIN dim1 AS d ON fp.r = d.k WHERE fp.r IN (0, 1000)" ${ON}
echo '--- T11 rule 2, dense integer: range 2500'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.dense = d.k" ${ON}
echo '--- T11 rule 2, sparse integer: range clamped to the rows'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.sparse = d.k" ${ON}

# Rule 3 divides the Parquet column chunk sizes, which depend on the encoder, so the expected value is derived.
# Prints whether `ResultRows` equals the rule 3 estimate of field `$1`. Usage: rule3 <field id> <query>.
rule3()
{
    local expected actual
    expected=$(${CLICKHOUSE_CLIENT} --query "
        SELECT toUInt64(1 / greatest(least(intDiv(sum(column_sizes[$1]), 8), 100000), 10) * 100000 * 10)
        FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'f'")
    actual=$(labels "$2" ${ON} | grep 'ResultRows')
    if [ "${actual}" = "ResultRows: ${expected}" ]; then
        echo "ResultRows equals the estimate from column_sizes / 8"
    else
        echo "${actual}, expected ${expected} from column_sizes / 8"
    fi
}

echo '--- T11 rule 3, Float64 without bounds'
rule3 3 "SELECT count() FROM f JOIN dim10 AS d ON f.fl = d.kf"
echo '--- T11 rule 3, Decimal(18, 2): bounds, but no range rule for decimals'
rule3 4 "SELECT count() FROM f JOIN dim10 AS d ON f.dc = d.kd"
echo '--- T11 rule 4, String: half the rows'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.s = d.ks" ${ON}
echo '--- T11 rule 2 in an Avro data file'
labels "SELECT count() FROM fa JOIN dim10 AS d ON fa.dense = d.k" ${ON}
echo '--- T11 rule 4, Float64 in an Avro data file without column_sizes: 30% of the rows'
labels "SELECT count() FROM fa JOIN dim10 AS d ON fa.fl = d.kf" ${ON}
echo '--- T17 use_statistics = 0: rows only'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.dense = d.k" ${NO_STATS}
echo '--- T17 use_iceberg_manifest_column_statistics = 0: rows only'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.dense = d.k" ${NO_COLUMN_STATS}
echo '--- gate off'
labels "SELECT count() FROM f JOIN dim10 AS d ON f.dense = d.k" ${OFF}

rm -rf "${LAKE}"
