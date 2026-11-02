#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files over HTTP from MinIO

# TopN dynamic filtering (`ORDER BY <column> LIMIT n`) for Parquet reads through the `url` table
# function and the `URL` engine. The filter drops rows and skips row groups that cannot enter the
# top-K, so fewer rows are read, and the results must not change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="${CLICKHOUSE_DATABASE}_05345"

# Single-threaded reading, so that the threshold is established from the first row groups of the
# sorted data before the rest is read.
SETTINGS="query_plan_max_limit_for_top_k_optimization = 1000, use_top_k_dynamic_filtering_for_variable_length_types = 0,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    max_threads = 1, max_parsing_threads = 1, use_query_condition_cache = 0"
ON="use_top_k_dynamic_filtering = 1"
OFF="use_top_k_dynamic_filtering = 0"

# Prints the result, and whether the query read fewer rows than `$2`.
function run_counting()
{
    ${CLICKHOUSE_CLIENT} --query "$1 SETTINGS ${SETTINGS}, ${ON} FORMAT JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'read fewer rows:', d['statistics']['rows_read'] < $2)"
}

# Compares the results with and without the optimization.
function compare()
{
    diff <(${CLICKHOUSE_CLIENT} --query "$1 SETTINGS ${SETTINGS}, ${ON}") \
         <(${CLICKHOUSE_CLIENT} --query "$1 SETTINGS ${SETTINGS}, ${OFF}") && echo "OK"
}

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/sorted.parquet', format = Parquet)
    SELECT number AS k, number * 10 AS v FROM numbers(200000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/other.parquet', format = Parquet)
    SELECT number + 1000000 AS k, number AS v FROM numbers(100000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000"

echo "--- url"
URL="url('http://localhost:11111/test/${S3_DIR}/sorted.parquet', 'Parquet')"
run_counting "SELECT k FROM ${URL} ORDER BY k LIMIT 3" 200000
for query in \
    "SELECT k, v FROM ${URL} ORDER BY k LIMIT 7" \
    "SELECT k, v FROM ${URL} ORDER BY k DESC LIMIT 7" \
    "SELECT k, v FROM ${URL} WHERE v % 40 = 0 ORDER BY k LIMIT 7" \
    "SELECT k, v FROM ${URL} ORDER BY k LIMIT 7 OFFSET 13" \
    "SELECT k, v FROM ${URL} ORDER BY k, v LIMIT 7" \
    "SELECT k, _file FROM ${URL} ORDER BY k DESC LIMIT 3"
do
    compare "$query"
done

echo "--- url with a glob over two files"
URLS="url('http://localhost:11111/test/${S3_DIR}/{sorted,other}.parquet', 'Parquet')"
compare "SELECT k, v FROM ${URLS} ORDER BY k DESC LIMIT 5"
compare "SELECT k, v FROM ${URLS} ORDER BY k LIMIT 5"
compare "SELECT _file, k FROM ${URLS} ORDER BY _file, k LIMIT 5"

echo "--- URL engine"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05345 (k UInt64, v UInt64) ENGINE = URL('http://localhost:11111/test/${S3_DIR}/sorted.parquet', 'Parquet')"
run_counting "SELECT k FROM t_05345 ORDER BY k DESC LIMIT 3" 200000
compare "SELECT k, v FROM t_05345 ORDER BY k DESC LIMIT 7"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05345"

echo "--- a column with a default expression is not filtered"
# The reader would compare its own placeholders for missing values against a threshold made from the
# recomputed ones. `w` is missing in the file, so every row gets the default.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05345_default (k UInt64, w UInt64 DEFAULT 1000000 - k) ENGINE = URL('http://localhost:11111/test/${S3_DIR}/sorted.parquet', 'Parquet')"
compare "SELECT k, w FROM t_05345_default ORDER BY w LIMIT 3"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05345_default"
