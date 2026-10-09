#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY ... LIMIT over a glob of Parquet files opens first the files whose statistics hold the best values of
# the sort key, so the top-K threshold skips the row groups of the other files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05341_parquet_topk_files_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# The checks depend on the number of streams, so `max_threads` must not be lowered under the memory limit of the test.
LOCAL=(${CLICKHOUSE_LOCAL}
    --max_threads_min_free_memory_per_thread=0
    --use_top_k_dynamic_filtering_for_variable_length_types=0
    --query_plan_max_limit_for_top_k_optimization=10000
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --max_block_size=65409)
ST=(--max_threads=1 --max_parsing_threads=1)
MT=(--max_threads=4 --max_parsing_threads=4)
ON=(--use_top_k_dynamic_filtering=1)
OFF=(--use_top_k_dynamic_filtering=0)

# 8 files of 4 row groups of 250 rows with disjoint ranges of k, not in the order of the file names: the
# smallest k are in f_1, the largest in f_6. Every row group has a NULL in m; f_3 has NULLs in n.
mkdir -p "${DIR}/t" "${DIR}/bad" "${DIR}/mixed" "${DIR}/empty" "${DIR}/hive/p=1" "${DIR}/hive/p=2" "${DIR}/where"
inserts=""
for j in $(seq 0 7); do
    inserts+="
        INSERT INTO FUNCTION file('${DIR}/t/f_${j}.parquet', Parquet)
        SELECT number + 1000 * ((${j} * 3 + 5) % 8) AS k, number * 8 + ${j} AS v,
            if(${j} = 3 AND number % 7 = 0, NULL, number) AS n, if(number % 100 = 0, NULL, k) AS m
        FROM numbers(1000)
        SETTINGS output_format_parquet_row_group_size = 250, engine_file_truncate_on_insert = 1;"
done
# where/a.parquet has the smallest k, but only in a row group that WHERE w = 1 prunes; where/b.parquet has the next ones.
inserts+="
    INSERT INTO FUNCTION file('${DIR}/where/a.parquet', Parquet)
    SELECT if(number < 250, number, number + 4750) AS k, toUInt8(number >= 250) AS w FROM numbers(500)
    SETTINGS output_format_parquet_row_group_size = 250, engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${DIR}/where/b.parquet', Parquet)
    SELECT number + 4000 AS k, toUInt8(1) AS w FROM numbers(250)
    SETTINGS output_format_parquet_row_group_size = 250, engine_file_truncate_on_insert = 1;"
"${LOCAL[@]}" --query "${inserts}
    SELECT count(), sum(num_row_groups) FROM file('${DIR}/t/f_*.parquet', ParquetMetadata)"

GLOB="file('${DIR}/t/f_*.parquet')"

echo "-- the file with the smallest (largest) k is opened first and the row groups of the others are skipped"
"${LOCAL[@]}" "${ST[@]}" "${ON[@]}" --format JSON --query "
    SELECT k FROM ${GLOB} ORDER BY k LIMIT 3;
    SELECT k FROM ${GLOB} ORDER BY k DESC LIMIT 3;
    SELECT k FROM ${GLOB} WHERE k >= 4000 ORDER BY k LIMIT 3;
    SELECT k FROM file('${DIR}/where/*.parquet') WHERE w = 1 ORDER BY k LIMIT 3" | python3 -c "
import sys, json
text, decoder, pos = sys.stdin.read(), json.JSONDecoder(), 0
while text[pos:].strip():
    while text[pos].isspace():
        pos += 1
    d, pos = decoder.raw_decode(text, pos)
    print([list(r.values()) for r in d['data']], 'rows read <= 250:', d['statistics']['rows_read'] <= 250)"

# A file without the sort column, Hive partitioning, an empty file skipped by the setting.
cp "${DIR}/t/f_1.parquet" "${DIR}/t/f_6.parquet" "${DIR}/mixed/"
cp "${DIR}/t/f_1.parquet" "${DIR}/t/f_2.parquet" "${DIR}/hive/p=1/"
cp "${DIR}/t/f_5.parquet" "${DIR}/t/f_6.parquet" "${DIR}/hive/p=2/"
cp "${DIR}"/t/f_*.parquet "${DIR}/empty/"
touch "${DIR}/empty/e.parquet"
"${LOCAL[@]}" --query "INSERT INTO FUNCTION file('${DIR}/mixed/w.parquet', Parquet) SELECT number AS v FROM numbers(1000, 5)"
MIXED="file('${DIR}/mixed/*.parquet', Parquet, 'k Int64, v Int64')"
HIVE="file('${DIR}/hive/*/*.parquet')"

echo "-- results identical with and without the optimization"
queries="
    SELECT k, v FROM ${GLOB} ORDER BY k LIMIT 7;
    SELECT k, v FROM ${GLOB} ORDER BY k DESC LIMIT 7;
    SELECT k, v FROM ${GLOB} ORDER BY k LIMIT 7 OFFSET 2600;
    SELECT k, v FROM ${GLOB} WHERE v % 3 = 0 ORDER BY k LIMIT 7;
    SELECT k, v FROM ${GLOB} WHERE k BETWEEN 2500 AND 5500 ORDER BY k DESC LIMIT 7;
    SELECT k, n FROM ${GLOB} ORDER BY n NULLS FIRST, k LIMIT 7;
    SELECT k, n FROM ${GLOB} ORDER BY n DESC, k LIMIT 7;
    SELECT k, m FROM ${GLOB} ORDER BY m, k LIMIT 7;
    SELECT k, v, _file FROM ${GLOB} ORDER BY k LIMIT 7;
    SELECT k, v FROM ${GLOB} WHERE _file IN ('f_2.parquet', 'f_5.parquet') ORDER BY k LIMIT 7;
    SELECT k, v FROM ${GLOB} WHERE _file IN (SELECT 'f_' || toString(number) || '.parquet' FROM numbers(2, 2)) ORDER BY k LIMIT 7;
    SELECT k, v FROM file('${DIR}/empty/*.parquet', Parquet, 'k Int64, v Int64') ORDER BY k LIMIT 7 SETTINGS engine_file_skip_empty_files = 1;
    SELECT p, k FROM ${HIVE} ORDER BY k DESC LIMIT 5 SETTINGS use_hive_partitioning = 1;
    SELECT p, k FROM ${HIVE} ORDER BY p, k LIMIT 5 SETTINGS use_hive_partitioning = 1;
    SELECT p, k FROM ${HIVE} WHERE p = 2 ORDER BY k LIMIT 5 SETTINGS use_hive_partitioning = 1;
    SELECT k, v FROM ${MIXED} ORDER BY k DESC, v LIMIT 7;
    SELECT k, v FROM ${MIXED} ORDER BY k, v LIMIT 3"
for threads in "${ST[*]}" "${MT[*]}"; do
    "${LOCAL[@]}" ${threads} "${ON[@]}" --query "${queries}" > "${DIR}/on.tsv"
    "${LOCAL[@]}" ${threads} "${OFF[@]}" --query "${queries}" > "${DIR}/off.tsv"
    diff "${DIR}/on.tsv" "${DIR}/off.tsv" && echo "OK"
done
echo "-- a file without the sort column"
tail -n 3 "${DIR}/on.tsv"

# $1: arguments for the number of threads, $2: sort column, $3: settings of the query. Prints the result, how often
# the readers found a footer in the metadata cache, and whether each footer was read from its file at most once.
cache_events() {
    "${LOCAL[@]}" "${ON[@]}" $1 --query "
        SELECT $2 FROM ${GLOB} ORDER BY $2 LIMIT 3 ${3:+SETTINGS $3};
        SELECT 'hits', (SELECT sum(value) FROM system.events WHERE event = 'ParquetMetadataCacheHits'),
            'misses <= 8', (SELECT sum(value) FROM system.events WHERE event = 'ParquetMetadataCacheMisses') <= 8"
}

echo "-- each footer is read once: by the first pass into the metadata cache, where the readers find it"
cache_events "${MT[*]}" k
cache_events "${ST[*]}" k
echo "-- the first pass stops after the first file when the sort column has no usable statistics"
cache_events "${ST[*]}" m
echo "-- no first pass with use_parquet_metadata_cache = 0, input_format_parquet_preserve_order = 1 or input_format_allow_seeks = 0"
cache_events "${MT[*]}" k "use_parquet_metadata_cache = 0"
cache_events "${MT[*]}" k "input_format_parquet_preserve_order = 1"
cache_events "${MT[*]}" k "input_format_allow_seeks = 0"

echo "-- a corrupt file and an empty file fail the query in the same way"
for j in 0 1 2 3; do cp "${DIR}/t/f_${j}.parquet" "${DIR}/bad/"; done
echo "not a parquet file" > "${DIR}/bad/f_9.parquet"
for args in "${ON[*]}" "${OFF[*]}"; do
    "${LOCAL[@]}" "${MT[@]}" ${args} --ignore-error --query "
        SELECT k FROM file('${DIR}/bad/*.parquet', Parquet, 'k Int64') ORDER BY k LIMIT 3;
        SELECT k FROM file('${DIR}/empty/*.parquet', Parquet, 'k Int64') ORDER BY k LIMIT 3" 2>&1 \
        | grep -o "Code: [0-9]*"
done
echo "-- EXPLAIN PIPELINE does not fail on the corrupt file"
"${LOCAL[@]}" "${MT[@]}" "${ON[@]}" --query "
    EXPLAIN PIPELINE SELECT k FROM file('${DIR}/bad/*.parquet', Parquet, 'k Int64') ORDER BY k LIMIT 3" > /dev/null && echo "OK"

echo "-- no first pass with at most one file per stream or with more than 4 streams"
mkdir -p "${DIR}/four" "${DIR}/many"
for j in 0 1 2 3; do cp "${DIR}/t/f_${j}.parquet" "${DIR}/four/"; done
cp "${DIR}"/t/f_*.parquet "${DIR}/many/"
cp "${DIR}/t/f_0.parquet" "${DIR}/many/g_0.parquet"
cp "${DIR}/t/f_1.parquet" "${DIR}/many/g_1.parquet"
for row in "four 4" "many 4" "many 5"; do
    read -r dir threads <<< "${row}"
    "${LOCAL[@]}" "${ON[@]}" --max_threads="${threads}" --max_parsing_threads="${threads}" --query "
        SELECT k FROM file('${DIR}/${dir}/*.parquet') ORDER BY k LIMIT 3 FORMAT Null;
        SELECT '${dir}', ${threads}, (SELECT sum(value) FROM system.events WHERE event = 'ParquetMetadataCacheHits')"
done
