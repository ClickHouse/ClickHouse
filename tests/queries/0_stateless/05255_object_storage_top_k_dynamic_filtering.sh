#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs S3 (MinIO) and Iceberg (USE_AVRO)

# TopN dynamic filtering (`ORDER BY <column> LIMIT n`) for Parquet reads from object storage: plain
# Parquet files on S3 and Iceberg tables. The filter drops rows and skips row groups that cannot enter
# the top-K, so fewer rows are read, and the results must not change. For Iceberg it must stay off for
# the files whose columns the data lake rewrites after the reader: a file written before a column was
# renamed stores another column under the name the query sorts by.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="${CLICKHOUSE_DATABASE}_05255"
ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05255"
rm -rf "${ICEBERG_DIR}"

# Parallel Iceberg sinks race for the metadata commit and log the conflict.
ICEBERG_INSERT_SETTINGS="SET allow_experimental_insert_into_iceberg = 1; SET max_insert_threads = 1;"

# Single-threaded reading, so that the threshold is established from the first row groups of the
# sorted data before the rest is read; no query condition cache, which would skip row groups too.
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

echo "--- Parquet on S3"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/sorted.parquet', format = Parquet)
    SELECT number AS k, number * 10 AS v FROM numbers(200000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000"
S3="s3(s3_conn, filename = '${S3_DIR}/sorted.parquet', format = Parquet)"
run_counting "SELECT k FROM ${S3} ORDER BY k LIMIT 3" 200000
for query in \
    "SELECT k, v FROM ${S3} ORDER BY k LIMIT 7" \
    "SELECT k, v FROM ${S3} ORDER BY k DESC LIMIT 7" \
    "SELECT k, v FROM ${S3} WHERE v % 40 = 0 ORDER BY k LIMIT 7" \
    "SELECT k, v FROM ${S3} ORDER BY k LIMIT 7 OFFSET 13" \
    "SELECT k, v FROM ${S3} ORDER BY k, v LIMIT 7"
do
    compare "$query"
done

echo "--- Iceberg"
${CLICKHOUSE_CLIENT} --query "
    ${ICEBERG_INSERT_SETTINGS}
    CREATE TABLE t_05255_sorted (k Int64, v Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}/sorted', 'Parquet');
    INSERT INTO t_05255_sorted SELECT number, number * 10 FROM numbers(200000)
    SETTINGS output_format_parquet_row_group_size = 10000;
"
run_counting "SELECT k FROM t_05255_sorted ORDER BY k LIMIT 3" 200000
compare "SELECT k, v FROM t_05255_sorted ORDER BY k DESC LIMIT 7"
compare "SELECT k, v FROM t_05255_sorted WHERE v % 40 = 0 ORDER BY k LIMIT 7"

echo "--- Iceberg: a file written before the sort column got its name"
# The first file stores `k` = number % 7 and `v` = number. After the renames, the query's `k` is the
# file's `v`, while the file still stores a column named `k`. The result of `ORDER BY k DESC` lies at
# the end of the file, and a threshold made from the query's `k` would reject every value of the
# file's own `k` there - both its rows and its row groups (by their statistics).
${CLICKHOUSE_CLIENT} --query "
    ${ICEBERG_INSERT_SETTINGS}
    CREATE TABLE t_05255_renamed (k Int64, v Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}/renamed', 'Parquet');
    INSERT INTO t_05255_renamed SELECT number % 7, number FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 10000;
"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE t_05255_renamed RENAME COLUMN k TO k_old;
"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE t_05255_renamed RENAME COLUMN v TO k;
"
${CLICKHOUSE_CLIENT} --query "
    ${ICEBERG_INSERT_SETTINGS}
    INSERT INTO t_05255_renamed (k_old, k) SELECT number, number FROM numbers(1000);
"
${CLICKHOUSE_CLIENT} --query "SELECT k, k_old FROM t_05255_renamed ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}, ${ON}"
compare "SELECT k, k_old FROM t_05255_renamed ORDER BY k DESC LIMIT 3"
compare "SELECT k, k_old FROM t_05255_renamed ORDER BY k LIMIT 3"

echo "--- Iceberg: sorted by an identity partition column"
${CLICKHOUSE_CLIENT} --query "
    ${ICEBERG_INSERT_SETTINGS}
    CREATE TABLE t_05255_partitioned (p Int64, k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}/partitioned', 'Parquet')
    PARTITION BY (p);
    INSERT INTO t_05255_partitioned SELECT number % 3, number FROM numbers(30000);
"
compare "SELECT p, k FROM t_05255_partitioned ORDER BY p DESC, k LIMIT 5"
compare "SELECT p, k FROM t_05255_partitioned ORDER BY p, k DESC LIMIT 5"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05255_sorted"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05255_renamed"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05255_partitioned"
rm -rf "${ICEBERG_DIR}"
