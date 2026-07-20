#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for reading Parquet files from object storage: for
# `ORDER BY ... LIMIT n` queries, the columns that are not needed for sorting and
# filtering are read only for the `n` rows that survive the `LIMIT`.
# The whole battery is run with the optimization enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data_1.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(0, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION s3('${URL}/data_2.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(1000, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION s3('${URL}/data_3.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s, range(number % 5) AS arr
    FROM numbers(2000, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

TABLE_FN="s3('${URL}/data_{1,2,3}.parquet', ${AUTH}, 'Parquet')"

QUERIES="
SELECT '-- simple ORDER BY ... LIMIT';
SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 5;
SELECT '-- ORDER BY ... DESC LIMIT across files';
SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k DESC LIMIT 7;
SELECT '-- with a filter (moved to PREWHERE)';
SELECT k, s FROM ${TABLE_FN} WHERE f = 3 ORDER BY k DESC LIMIT 4;
SELECT '-- expressions on lazy columns are applied after the LIMIT';
SELECT length(s) + f AS x, upper(s) FROM ${TABLE_FN} ORDER BY intDiv(k, 100) DESC, k LIMIT 3;
SELECT '-- sorting by a column with duplicate values';
SELECT f, s FROM ${TABLE_FN} ORDER BY f, k LIMIT 5;
SELECT '-- virtual columns together with lazy columns';
SELECT _file, _row_number, k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3 OFFSET 998;
SELECT '-- the number of lazy read steps in the plan';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3);
SELECT '-- lazily read columns are shown in EXPLAIN';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_object_storage = $enabled"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "$QUERIES"
done
