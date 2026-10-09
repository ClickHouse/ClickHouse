#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY ... LIMIT over Parquet: a row group admitted before the top-K threshold excluded it is
# skipped when its column data is scheduled, if that data waits for a decoding thread meanwhile.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05346"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

# 40 row groups of 2000 rows with disjoint ranges of k, not in file order; p = 1 in the second half
# of every row group.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/t.parquet', Parquet)
    SELECT k, number AS v, toUInt8(pos >= 1000) AS p, leftPad(toString(k), 6, '0') AS s
    FROM (SELECT number, number % 2000 AS pos, ((intDiv(number, 2000) * 7 + 3) % 40) * 2000 + pos AS k FROM numbers(80000))
    SETTINGS output_format_parquet_row_group_size = 2000, max_block_size = 2000, engine_file_truncate_on_insert = 1"

# Backdate the file so its version token has settled and the query condition cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/t.parquet"

FILE="file('${DATA_DIR}/t.parquet', Parquet, 'k UInt64, v UInt64, p UInt8')"
FORMAT_SETTINGS="input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    input_format_parquet_preserve_order = 0, input_format_parquet_memory_low_watermark = 0"
COMMON="query_plan_max_limit_for_top_k_optimization = 1000, ${FORMAT_SETTINGS}"
EVENTS="${CLICKHOUSE_TMP}/05346_events"

# Runs the query $1, prints its result on one line and keeps its profile events in ${EVENTS}.
run_query()
{
    ${CLICKHOUSE_CLIENT} --print-profile-events --profile-events-delay-ms=-1 --query "$1" 2> "${EVENTS}" | paste -s -d ' ' | tr '\t' ' '
}

# Prints the value of the profile event $1 of the last query run by run_query.
event()
{
    grep -oE "\[ 0 \] $1: [0-9]+" "${EVENTS}" | tail -1 | grep -oE '[0-9]+$' || echo 0
}

echo "-- results in four configurations"
queries=(
    "SELECT k, v FROM ${FILE} ORDER BY k LIMIT 3"
    "SELECT k, v FROM ${FILE} ORDER BY k DESC LIMIT 3"
    "SELECT k, v FROM ${FILE} WHERE p = 1 ORDER BY k LIMIT 3"
)
# The number of configurations that returned the same rows, then the rows, for each query in order.
for query in "${queries[@]}"; do
    for cfg in "max_threads = 1, max_parsing_threads = 2, input_format_parquet_max_block_size = 100" \
        "max_threads = 4, max_parsing_threads = 8, input_format_parquet_max_block_size = 4000"; do
        for prewhere in 1 0; do
            echo "${query} SETTINGS ${cfg}, optimize_move_to_prewhere = ${prewhere}, query_plan_optimize_prewhere = ${prewhere},
                use_query_condition_cache = 0, use_top_k_dynamic_filtering = 1, ${COMMON};"
        done
    done
done | ${CLICKHOUSE_CLIENT} | paste - - - | uniq -c | awk '{$1 = $1; print}'

echo "-- row groups excluded while their data waits are not read"
# 2 parsing threads admit 3 row groups before there is a threshold, and the decoding stage then runs one
# batch at a time, so the 2nd and 3rd ones wait behind the 20 row subgroups of the 1st one. Without the
# late check at most 40 - 3 = 37 row groups are skipped. The race is repeated until it is won, at most 20 times.
# The sort column has one page per row group: a skipped row group reads none, so skipped plus read pages is 40.
late_skip()
{
    for _ in $(seq 1 20); do
        result=$(run_query "$1")
        if [ "$(event ParquetTopKSkippedRowGroups)" -ge 38 ]; then
            echo "${result}, skipped plus read pages: $(($(event ParquetTopKSkippedRowGroups) + $(event ParquetReadPages)))"
            return
        fi
    done
    echo "no read skipped 38 row groups"
}
late_skip "SELECT k FROM ${FILE} ORDER BY k LIMIT 3
    SETTINGS max_threads = 1, max_parsing_threads = 2, input_format_parquet_max_block_size = 100,
        use_top_k_dynamic_filtering = 1, use_query_condition_cache = 0, ${COMMON}"
# The String key needs the per-row filter enabled.
late_skip "SELECT s FROM file('${DATA_DIR}/t.parquet', Parquet, 's String') ORDER BY s LIMIT 3
    SETTINGS max_threads = 1, max_parsing_threads = 2, input_format_parquet_max_block_size = 100,
        use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1,
        use_query_condition_cache = 0, ${COMMON}"

echo "-- the query condition cache after a read that skipped row groups late"
# A File table takes the format settings from its definition, not from the query.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05346;
    CREATE TABLE t_05346 (k UInt64, v UInt64, p UInt8, s String) ENGINE = File(Parquet, '${DATA_DIR}/t.parquet')
    SETTINGS input_format_parquet_max_block_size = 100, ${FORMAT_SETTINGS}"
CACHE="max_threads = 1, max_parsing_threads = 2, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000"
# Each attempt has its own predicate, so its own cache entry: the second read must use the entry the first one wrote.
cached=""
for a in $(seq 1 20); do
    query="SELECT k, v FROM t_05346 WHERE p = 1 AND v != ${a}000000 ORDER BY k LIMIT 3
        SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache = 1, ${CACHE}"
    written=$(run_query "${query}")
    if [ "$(event ParquetTopKSkippedRowGroups)" -ge 38 ]; then
        cached="${written}; $(run_query "${query}"), cache hit: $(($(event QueryConditionCacheHits) > 0))"
        break
    fi
done
echo "${cached:-no read skipped 38 row groups}"

echo "-- a String key at the default settings: the query condition cache after the reads"
# A read that skips row groups must not leave a cache entry that hides rows from a later read with the same predicate.
# Such reads exist only where the row groups of a String key are skipped by their statistics without the per-row filter.
STRING_KEY="SELECT s FROM t_05346 WHERE p = 1 ORDER BY s LIMIT 3
    SETTINGS max_threads = 1, max_parsing_threads = 2, use_top_k_dynamic_filtering = 1,
        use_top_k_dynamic_filtering_for_variable_length_types = 0, use_query_condition_cache = 1,
        optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_max_limit_for_top_k_optimization = 1000"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/s.parquet', Parquet) SELECT toString(number) AS s FROM numbers(4)
    SETTINGS output_format_parquet_row_group_size = 1, engine_file_truncate_on_insert = 1"
run_query "SELECT s FROM file('${DATA_DIR}/s.parquet', Parquet, 's String') ORDER BY s LIMIT 1
    SETTINGS max_threads = 1, max_parsing_threads = 1, use_top_k_dynamic_filtering = 1,
        use_top_k_dynamic_filtering_for_variable_length_types = 0, ${COMMON}" > /dev/null
echo "skipped by statistics: $(($(event ParquetTopKSkippedRowGroups) > 0))"
if [ "$(event ParquetTopKSkippedRowGroups)" -gt 0 ]; then
    for _ in $(seq 1 5); do
        ${CLICKHOUSE_CLIENT} --query "${STRING_KEY}" > /dev/null
    done
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05346 WHERE p = 1 SETTINGS use_query_condition_cache = 1"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
