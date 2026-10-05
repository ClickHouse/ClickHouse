#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: a manifest stores a decimal partition value as unscaled bytes. Both the planning-time pruning and rule 1
# of the column statistics (distinct identity-partition values) decode it with the column type. 3 files, one per value
# of `d`, 10000 rows each.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=1"
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

${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE fd (d Decimal(9, 2), v Int64) ENGINE = IcebergLocal('${LAKE}/fd') PARTITION BY (d);
    INSERT INTO fd SELECT [1.50, 2.25, 3.00][number % 3 + 1], number FROM numbers(30000);
    CREATE TABLE dim1 (k Decimal(9, 2)) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim1 SELECT 2.25;
"

echo '--- fixture: data files and their partition values'
${CLICKHOUSE_CLIENT} --query "
    SELECT partition, record_count FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'fd' ORDER BY partition"
echo '--- pruning by the decoded partition value: 1 of 3 files remains'
labels "SELECT count() FROM fd JOIN dim1 AS d ON fd.d = d.k WHERE fd.d = toDecimal32('2.25', 2)" ${ON}
echo '--- the read returns the rows of that file'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM fd WHERE d = toDecimal32('2.25', 2)"
echo '--- rule 1 counts the 3 decoded partition values: 30000 * 1 / 3'
labels "SELECT count() FROM fd JOIN dim1 AS d ON fd.d = d.k" ${ON}
echo '--- without column statistics the row count stands in for the NDV: 30000 * 1 / 30000'
labels "SELECT count() FROM fd JOIN dim1 AS d ON fd.d = d.k" ${NO_COLUMN_STATS}

rm -rf "${LAKE}"
