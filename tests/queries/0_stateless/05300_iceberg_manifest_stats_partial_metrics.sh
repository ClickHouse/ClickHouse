#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: column statistics from Iceberg manifests over data files whose metrics differ.
# T13: a file where the column is all NULL, or whose schema predates the column, counts as NULLs and keeps the
# min/max of the other files; a statistic that one file lacks (`column_sizes` of an Avro data file, bounds of
# `Float64`) is not published, so the NDV falls to the next rule.
# T14: bounds written before `MODIFY COLUMN k Int64` are 4 bytes, decoded with the type of the file's schema, and a
# renamed column keeps its field id: the statistics equal those of a twin that was `Int64` from the start.
# T17: with `use_statistics = 0` the read reports its rows only.
# Each join with a MergeTree table of 10 distinct keys estimates `rows_ice * 10 / max(ndv_ice, 10)` rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

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

# Prints the column entries of relation t from its `Estimated statistics` trace line, sorted (the map is unordered).
column_entries()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} "$@" --query "EXPLAIN ${query}" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ t: .*' | head -n 1 \
        | grep -oE '__table1\.[a-z0-9_]+: [0-9]+( \[[^]]*\])?' | LC_ALL=C sort
}

# Every table gets two data files of 1000 rows.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE an (k Int64, n Nullable(Int64)) ENGINE = IcebergLocal('${LAKE}/an');
    INSERT INTO an SELECT number, NULL FROM numbers(1000);
    INSERT INTO an SELECT number + 1000, number % 100 FROM numbers(1000);
    CREATE TABLE ac (k Int64) ENGINE = IcebergLocal('${LAKE}/ac');
    INSERT INTO ac SELECT number FROM numbers(1000);
    ALTER TABLE ac ADD COLUMN c Nullable(Int64);
    INSERT INTO ac SELECT number + 1000, number % 50 FROM numbers(1000);
    CREATE TABLE mx (k Int64, f Float64) ENGINE = IcebergLocal('${LAKE}/mx');
    INSERT INTO mx SELECT number, number FROM numbers(1000);
    INSERT INTO FUNCTION icebergLocal('${LAKE}/mx', 'Avro') SELECT toInt64(number + 1000) AS k, toFloat64(number) AS f
        FROM numbers(1000);
    CREATE TABLE pr (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/pr');
    INSERT INTO pr SELECT number % 500, number FROM numbers(1000);
    ALTER TABLE pr MODIFY COLUMN k Int64;
    INSERT INTO pr SELECT number % 500 + 500, number + 1000 FROM numbers(1000);
    ALTER TABLE pr RENAME COLUMN v TO w;
    CREATE TABLE pr_twin (k Int64, w Int64) ENGINE = IcebergLocal('${LAKE}/pr_twin');
    INSERT INTO pr_twin SELECT number % 500, number FROM numbers(1000);
    INSERT INTO pr_twin SELECT number % 500 + 500, number + 1000 FROM numbers(1000);
    CREATE TABLE dim10 (k Int64, kf Float64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number, number FROM numbers(10);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"
echo '--- fixture: NULL counts of an.n per file; whether a file of ac has a metric for c; format and column_sizes of mx'
${CLICKHOUSE_CLIENT} --query "
    SELECT arraySort(groupArray(null_value_counts[2])) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'an'"
${CLICKHOUSE_CLIENT} --query "
    SELECT arraySort(groupArray(mapContains(null_value_counts, 2))) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'ac'"
${CLICKHOUSE_CLIENT} --query "
    SELECT arraySort(groupArray((upper(file_format), mapContains(column_sizes, 2)))) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'mx'"

for CASE in \
    "T13 an: an all-NULL file counts as NULLs, not as a file without bounds|SELECT t.k FROM an AS t JOIN dim10 AS d ON t.n = d.k" \
    "T13 ac: a file written before ADD COLUMN counts as NULLs|SELECT t.k FROM ac AS t JOIN dim10 AS d ON t.c = d.k" \
    "T13 mx: no column_sizes in the Avro file and no bounds for Float64, so f takes rule 4|SELECT t.k FROM mx AS t JOIN dim10 AS d ON t.f = d.kf" \
    "T14 pr: MODIFY COLUMN k Int64 between the inserts, then RENAME COLUMN v TO w|SELECT t.w FROM pr AS t JOIN dim10 AS d ON t.k = d.k" \
    "T14 pr_twin: Int64 from the start|SELECT t.w FROM pr_twin AS t JOIN dim10 AS d ON t.k = d.k"
do
    echo "--- ${CASE%%|*}"
    labels "${CASE#*|}" ${ON}
    column_entries "${CASE#*|}" ${ON}
done

T17="SELECT t.w FROM pr AS t JOIN dim10 AS d ON t.k = d.k"
echo '--- T17 use_statistics = 0: rows only'
labels "${T17}" ${NO_STATS}
echo "column entries: $(column_entries "${T17}" ${NO_STATS} | wc -l)"

rm -rf "${LAKE}"
