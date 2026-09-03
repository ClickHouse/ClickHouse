#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the interaction of the single-file Parquet split with lazy
# materialization for `File`: when an `ORDER BY ... LIMIT` query splits one file across
# several bucketed sources, every source registers the same file generation in the
# `LazyFileRegistry`, and the deferred pass must reread the file once for all of them
# (one shared registry entry), not once per bucket.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05043_split_lazy_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${LOCAL_DIR}/data.parquet', Parquet)
    SELECT number AS k, arrayStringConcat(arrayMap(i -> hex(cityHash64(number, i)), range(64))) AS payload
    FROM numbers(4000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

# The main pass of lazy materialization projects only the ordering key, so the size gate is
# lowered to make even that narrow read split across the 40 row groups.
SPLIT_SETTINGS=(
    --parallelize_output_from_storages=1
    --max_threads=8
    --input_format_parquet_min_bytes_to_split=1
    --input_format_parquet_bytes_per_split_bucket=1
)
LAZY_SETTINGS=(
    --enable_analyzer=1
    --query_plan_optimize_lazy_materialization=1
    --query_plan_max_limit_for_lazy_materialization=0
    --query_plan_optimize_lazy_materialization_for_file=1
)
# Ordering by a hash spreads the surviving rows across row groups, so before the fix the
# deferred pass saw one registry entry per bucket that produced a survivor.
QUERY="SELECT k, cityHash64(payload) FROM file('${LOCAL_DIR}/data.parquet', Parquet, 'k UInt64, payload String') ORDER BY intHash64(k) LIMIT 5"

# Both features must engage at once: the plan grows the lazy branch and the pipeline
# multiplies the bucketed source.
echo "-- lazy branch in the plan"
"${LOCAL[@]}" "${SPLIT_SETTINGS[@]}" "${LAZY_SETTINGS[@]}" --query "EXPLAIN ${QUERY}" | grep -c 'LazilyReadFromFile'
echo "-- split sources in the pipeline"
"${LOCAL[@]}" "${SPLIT_SETTINGS[@]}" "${LAZY_SETTINGS[@]}" --query "EXPLAIN PIPELINE ${QUERY}" | grep -c 'File × '

# The deferred pass must see the split file as a single registry entry (a single reread).
echo "-- deferred pass rereads the file once"
"${LOCAL[@]}" "${SPLIT_SETTINGS[@]}" "${LAZY_SETTINGS[@]}" --send_logs_level=trace --query "${QUERY}" 2>&1 \
    | grep -oc 'Lazily reading [0-9]* rows from 1 files'

# The results must be identical with and without each of the two optimizations.
echo "-- split + lazy"
"${LOCAL[@]}" "${SPLIT_SETTINGS[@]}" "${LAZY_SETTINGS[@]}" --query "${QUERY}"
echo "-- split only"
"${LOCAL[@]}" "${SPLIT_SETTINGS[@]}" --query "${QUERY}"
echo "-- lazy only"
"${LOCAL[@]}" "${LAZY_SETTINGS[@]}" --parallelize_output_from_storages=0 --query "${QUERY}"
echo "-- neither"
"${LOCAL[@]}" --parallelize_output_from_storages=0 --query "${QUERY}"
