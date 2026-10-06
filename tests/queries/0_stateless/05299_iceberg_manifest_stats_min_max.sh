#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the min/max and NULL fraction of an Iceberg read with `use_iceberg_manifest_column_statistics`, from the
# manifest bounds and NULL counts.
# - T1: `ie_join` picks its two key conditions by the min/max (port of `05023` to Iceberg).
# - T2: the `Estimated statistics` trace line: rows, then NDV [min, max, NULL fraction] per column.
# - T3: files without values of a column count as NULLs, and its NDV is clamped to the non-NULL rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
# `query_plan_optimize_join_order_limit` is passed per call, since a client flag cannot be repeated.
PINS="--query_plan_optimize_join_order_randomize=0
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1 --session_timezone=UTC
    --use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=1"
# The printed conditions are mirrored when the join order optimizer swaps the sides, so it is off for `ie_join`.
IE_JOIN="--join_algorithm=ie_join --join_use_nulls=0 --query_plan_optimize_join_order_limit=0"
REORDER="--query_plan_optimize_join_order_limit=10"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# sel(a1 < b1) ~ 0.5, sel(a2 < b2) = 1, sel(a3 < b3) ~ 0.005: the best key pair is (a1 < b1, a3 < b3).
# nu: a file written before `ADD COLUMN c`, a file where c is NULL, and 2000 values of c in [0, 3998].
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE sel_l (a1 Int64, a2 Int64, a3 Int64) ENGINE = IcebergLocal('${LAKE}/sel_l');
    INSERT INTO sel_l SELECT number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
    CREATE TABLE sel_r (b1 Int64, b2 Int64, b3 Int64) ENGINE = IcebergLocal('${LAKE}/sel_r');
    INSERT INTO sel_r SELECT number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);
    CREATE TABLE nf (k Int64, n Nullable(Int64), d Date32, ts DateTime64(6), dc Decimal(10, 2), s String)
        ENGINE = IcebergLocal('${LAKE}/nf');
    INSERT INTO nf SELECT number, if(number % 4 = 0, NULL, number % 100), toDate32('2020-01-01') + number % 365,
        toDateTime64('2020-01-01 00:00:00', 6) + number, number / 4, toString(number) FROM numbers(1000);
    CREATE TABLE nu (k Int64) ENGINE = IcebergLocal('${LAKE}/nu');
    INSERT INTO nu SELECT number FROM numbers(1000);
    ALTER TABLE nu ADD COLUMN c Nullable(Int64);
    INSERT INTO nu SELECT number + 1000, NULL FROM numbers(1000);
    INSERT INTO nu SELECT number + 2000, number * 2 FROM numbers(2000);
    CREATE TABLE dim10 (k Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number FROM numbers(10);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

# Expect a1 < b1 AND a3 < b3, the two most selective by the min/max; without statistics a1, a2 in syntax order.
echo '--- T1: ie_join keys chosen by the min/max from the manifests'
${CLICKHOUSE_CLIENT} ${PINS} ${IE_JOIN} --query "
    SELECT extract(explain, 'Conditions: .*') FROM (EXPLAIN actions = 1
        SELECT count() FROM sel_l AS l JOIN sel_r AS r ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3)
    WHERE explain LIKE '%Conditions:%'"

# Prints the `Estimated statistics` trace line of relation a: its rows, then its column entries sorted (the map is
# unordered). Usage: relation_a <query>.
relation_a()
{
    local line
    line=$(${CLICKHOUSE_CLIENT_TRACE} ${PINS} ${REORDER} --query "EXPLAIN $1" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ a: .*' | head -n 1)
    echo "${line}" | grep -oE 'a: [0-9a-z]+ rows'
    echo "${line}" | grep -oE '__table1\.[a-z0-9_]+: [0-9]+( \[[^]]*\])?' | LC_ALL=C sort
}

# Rule 3 divides the Parquet column chunk size of `dc` (field id 5), which depends on the encoder.
DC_NDV=$(${CLICKHOUSE_CLIENT} --query "
    SELECT greatest(least(intDiv(sum(column_sizes[5]), 8), 1000), 1)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'nf'")

# Expect NDV k 1000, d 365, n 99 (75 true) by range, dc column_sizes / 8, s 10% of rows, ts range clamped to rows;
# n is NULL every 4th row (0.25); bounds as MergeTree keeps them: Date32 in days (18262 = 2020-01-01), ts in seconds.
echo '--- T2: rows, then NDV [min, max, NULL fraction] per column'
relation_a "SELECT a.n, a.d, a.ts, a.dc, a.s FROM nf AS a JOIN dim10 AS d ON a.k = d.k" \
    | sed "s/__table1\.dc: ${DC_NDV} /__table1.dc: <column_sizes \/ 8> /"

# Expect c 2000, null 0.5: 2000 of the 4000 rows count as NULLs, and the range 3999 is clamped to the 2000 others.
echo '--- T3: a file before ADD COLUMN and an all-NULL file, then values'
relation_a "SELECT a.c FROM nu AS a JOIN dim10 AS d ON a.k = d.k"

rm -rf "${LAKE}"
