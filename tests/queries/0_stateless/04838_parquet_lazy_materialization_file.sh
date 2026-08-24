#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for reading local Parquet files with the `file` table function and the
# `File` table engine: for `ORDER BY ... LIMIT n` queries, the columns that are not needed for
# sorting and filtering are read only for the `n` rows that survive the `LIMIT`.
# The whole battery is run with the optimization enabled and disabled; the results must match.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04838_lazy_mat_file_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data_1.parquet', Parquet)
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(0, 1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION file('${DATA_DIR}/data_2.parquet', Parquet)
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(1000, 1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION file('${DATA_DIR}/data_3.parquet', Parquet)
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(2000, 1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION file('${DATA_DIR}/data.csv', CSVWithNames)
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(0, 100)
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DATA_DIR}/json.parquet', Parquet, 'k UInt64, j JSON')
    SELECT number, toJSONString(map('user', map('name', concat('u', toString(number)), 'age', number)))
    FROM numbers(0, 1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

(
    cd "$DATA_DIR" || exit 1
    zip -0 -q archive.zip data_1.parquet
)

FIFO_PATH="${DATA_DIR}/data.parquet.fifo"
mkfifo "$FIFO_PATH"

TABLE_FN="file('${DATA_DIR}/data_{1,2,3}.parquet', Parquet)"

QUERIES="
SELECT '-- simple ORDER BY ... LIMIT';
SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 5;
SELECT '-- ORDER BY ... DESC LIMIT across files';
SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k DESC LIMIT 7;
SELECT '-- with a filter (moved to PREWHERE)';
SELECT k, s FROM ${TABLE_FN} WHERE f = 3 ORDER BY k DESC LIMIT 4;
SELECT '-- a preserved PREWHERE expression stays on the main branch';
SELECT f + 1, s FROM ${TABLE_FN} WHERE f + 1 > 0 ORDER BY k LIMIT 3;
SELECT '-- expressions on lazy columns are applied after the LIMIT';
SELECT length(s) + f AS x, upper(s) FROM ${TABLE_FN} ORDER BY intDiv(k, 100) DESC, k LIMIT 3;
SELECT '-- sorting by a column with duplicate values';
SELECT f, s FROM ${TABLE_FN} ORDER BY f, k LIMIT 5;
SELECT '-- virtual columns together with lazy columns';
SELECT _file, _row_number, k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3 OFFSET 998;
SELECT '-- the number of lazy read steps in the plan';
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3);
SELECT '-- lazily read columns are shown in EXPLAIN';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- a JSON subcolumn is deferred through its parent column';
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT k, j.user.name FROM file('${DATA_DIR}/json.parquet', Parquet, 'k UInt64, j JSON') ORDER BY k DESC LIMIT 3);
SELECT k, j.user.name FROM file('${DATA_DIR}/json.parquet', Parquet, 'k UInt64, j JSON') ORDER BY k DESC LIMIT 3;
SELECT '-- a deferred parent column whose subcolumn is a sort key input';
SELECT j FROM file('${DATA_DIR}/json.parquet', Parquet, 'k UInt64, j JSON') ORDER BY j.user.age.:Int64 DESC LIMIT 2;
SELECT '-- a non-Parquet file stays on the single-pass plan';
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT s FROM file('${DATA_DIR}/data.csv', CSVWithNames) ORDER BY k LIMIT 3);
SELECT k, s FROM file('${DATA_DIR}/data.csv', CSVWithNames) ORDER BY k DESC LIMIT 2;
SELECT '-- an archive entry stays on the single-pass plan';
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT s FROM file('${DATA_DIR}/archive.zip :: data_1.parquet', Parquet) ORDER BY k LIMIT 3);
SELECT k, s FROM file('${DATA_DIR}/archive.zip :: data_1.parquet', Parquet) ORDER BY k DESC LIMIT 2;
SELECT '-- a FIFO stays on the single-pass plan';
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT s FROM file('${FIFO_PATH}', Parquet, 'k UInt64, s String') ORDER BY k LIMIT 3);
SELECT '-- the File engine takes the lazy path as well';
CREATE TABLE t_lazy_mat_file (k UInt64, f UInt64, s String, arr Array(UInt64)) ENGINE = File(Parquet);
INSERT INTO t_lazy_mat_file SELECT number, number % 17, concat('engine_', toString(number)), range(number % 3) FROM numbers(1000);
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT s FROM t_lazy_mat_file ORDER BY k LIMIT 3);
SELECT k, s, arr FROM t_lazy_mat_file ORDER BY k DESC LIMIT 3;
DROP TABLE t_lazy_mat_file;
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_file = $enabled"
    "${LOCAL[@]}" \
        --enable_analyzer=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_file="$enabled" \
        --query "$QUERIES"
done
