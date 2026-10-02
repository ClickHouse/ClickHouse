#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_statistics`, `use_iceberg_manifest_column_statistics` and `use_statistics`
# an Iceberg read reports the min/max and the NULL fraction of its columns from the manifest bounds and NULL counts.
# T12: `ie_join` picks its two key conditions by the min/max selectivity (port of `05023` to Iceberg `Int64`
# columns); without column statistics it takes the first two in syntax order. Every choice returns the same result.
# T15: the join graph's `Estimated statistics` trace line of the Iceberg relation carries the same min/max and NULL
# fraction as MergeTree `basic` statistics of the same data, and the NDV of the chain rules. Every file pruned and a
# filter that prunes nothing give no column statistics.
# T17: with `use_statistics = 0` the read reports its rows only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
# `use_statistics` and `query_plan_optimize_join_order_limit` are passed per call, since a client flag cannot be repeated.
PINS="--query_plan_optimize_join_order_randomize=0
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1 --session_timezone=UTC"
ON="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=1"
NO_STATS="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1 --use_statistics=0"
OFF="--use_iceberg_manifest_statistics=0 --use_statistics=1"
# The printed conditions are mirrored when the join order optimizer swaps the sides, so it is off for `ie_join`.
IE_JOIN="--join_algorithm=ie_join --join_use_nulls=0 --query_plan_optimize_join_order_limit=0"
REORDER="--query_plan_optimize_join_order_limit=10"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# sel(a1 < b1) ~ 0.5, sel(a2 < b2) = 1, sel(a3 < b3) ~ 0.005: the best key pair is (a1 < b1, a3 < b3).
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE sel_l (a1 Int64, a2 Int64, a3 Int64) ENGINE = IcebergLocal('${LAKE}/sel_l');
    INSERT INTO sel_l SELECT number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
    CREATE TABLE sel_r (b1 Int64, b2 Int64, b3 Int64) ENGINE = IcebergLocal('${LAKE}/sel_r');
    INSERT INTO sel_r SELECT number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);
    CREATE TABLE nf (k Int64, n Nullable(Int64), d Date32, ts DateTime64(6), dc Decimal(10, 2), s String)
        ENGINE = IcebergLocal('${LAKE}/nf');
    INSERT INTO nf SELECT number, if(number % 4 = 0, NULL, number % 100), toDate32('2020-01-01') + number % 365,
        toDateTime64('2020-01-01 00:00:00', 6) + number, number / 4, toString(number) FROM numbers(1000);
    CREATE TABLE nf_mt (k Int64, n Nullable(Int64), d Date32, ts DateTime64(6), dc Decimal(10, 2), s String)
        ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
    INSERT INTO nf_mt SELECT number, if(number % 4 = 0, NULL, number % 100), toDate32('2020-01-01') + number % 365,
        toDateTime64('2020-01-01 00:00:00', 6) + number, number / 4, toString(number) FROM numbers(1000);
    CREATE TABLE dim10 (k Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO dim10 SELECT number FROM numbers(10);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

T12="SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM sel_l AS l JOIN sel_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3"

# Prints the key conditions `ie_join` chose. Usage: conditions [client flags].
conditions()
{
    ${CLICKHOUSE_CLIENT} ${PINS} ${IE_JOIN} "$@" --query "
        SELECT extract(explain, 'Conditions: .*') FROM (EXPLAIN actions = 1 ${T12}) WHERE explain LIKE '%Conditions:%'"
}

echo '--- T12 gate on: keys chosen by the min/max from the manifests'
conditions ${ON}
echo '--- T12 gate off: the first two in syntax order'
conditions ${OFF}
echo '--- T17 use_statistics = 0: the first two in syntax order'
conditions ${NO_STATS}
echo '--- T12 results are independent of the choice and equal the filter over CROSS JOIN'
${CLICKHOUSE_CLIENT} ${PINS} ${IE_JOIN} ${ON} --query "${T12}"
${CLICKHOUSE_CLIENT} ${PINS} ${IE_JOIN} ${OFF} --query "${T12}"
${CLICKHOUSE_CLIENT} ${PINS} ${IE_JOIN} ${ON} --query "
    SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM sel_l AS l, sel_r AS r
    WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3 SETTINGS join_algorithm = 'hash'"

# Prints the `Estimated statistics` trace line of relation a: its rows, then its column entries sorted (the map is
# unordered). Usage: relation_a <query> [client flags].
relation_a()
{
    local query="$1"
    shift
    local line
    line=$(${CLICKHOUSE_CLIENT_TRACE} ${PINS} ${REORDER} "$@" --query "EXPLAIN ${query}" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ a: .*' | head -n 1)
    echo "${line}" | grep -oE 'a: [0-9a-z]+ rows'
    echo "${line}" | grep -oE '__table1\.[a-z0-9_]+: [0-9]+( \[[^]]*\])?' | LC_ALL=C sort
}

T15="SELECT a.n, a.d, a.ts, a.dc, a.s FROM nf AS a JOIN dim10 AS d ON a.k = d.k"
T15_TWIN="SELECT a.n, a.d, a.ts, a.dc, a.s FROM nf_mt AS a JOIN dim10 AS d ON a.k = d.k"

# Rule 3 divides the Parquet column chunk size of `dc` (field id 5), which depends on the encoder.
DC_NDV=$(${CLICKHOUSE_CLIENT} --query "
    SELECT greatest(least(intDiv(sum(column_sizes[5]), 8), 1000), 1)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'nf'")

echo '--- T15 gate on: rows, then NDV [min, max, NULL fraction] per column'
ICEBERG_ENTRIES=$(relation_a "${T15}" ${ON})
echo "${ICEBERG_ENTRIES}" | sed "s/__table1\.dc: ${DC_NDV} /__table1.dc: <column_sizes \/ 8> /"
echo '--- T15 MergeTree twin with basic statistics, NDV removed'
TWIN_ENTRIES=$(relation_a "${T15_TWIN}" ${ON} | grep -F '__table1.' | sed -E 's/: [0-9]+/:/')
echo "${TWIN_ENTRIES}"
if [ "$(echo "${ICEBERG_ENTRIES}" | grep -F '__table1.' | sed -E 's/: [0-9]+/:/')" = "${TWIN_ENTRIES}" ]; then
    echo "min, max and NULL fraction equal the twin's: yes"
else
    echo "min, max and NULL fraction equal the twin's: no"
fi
echo '--- T15 every file pruned: no column statistics'
relation_a "${T15} WHERE a.k > 1000000" ${ON}
echo '--- T15 a filter that prunes nothing: unknown rows, no column statistics'
relation_a "${T15} WHERE a.k % 2 = 0" ${ON}
echo '--- T17 use_statistics = 0: rows only'
relation_a "${T15}" ${NO_STATS}

rm -rf "${LAKE}"
