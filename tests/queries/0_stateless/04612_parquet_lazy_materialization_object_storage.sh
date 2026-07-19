#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for reading Parquet files from object storage: for
# `ORDER BY ... LIMIT n` queries, the columns that are not needed for sorting and
# filtering are read only for the `n` rows that survive the `LIMIT`.
# Every query is run with the optimization enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat"
AUTH="'test', 'testtest'"

for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3('${URL}/data_${i}.parquet', ${AUTH}, 'Parquet')
        SELECT
            number AS k,
            number % 17 AS f,
            concat('val_', toString(number)) AS s,
            range(number % 5) AS arr
        FROM numbers(($i - 1) * 10000, 10000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000
    "
done

TABLE_FN="s3('${URL}/data_{1,2,3}.parquet', ${AUTH}, 'Parquet')"

run_with_both_settings()
{
    local query=$1
    for enabled in 1 0; do
        ${CLICKHOUSE_CLIENT} \
            --query_plan_optimize_lazy_materialization=1 \
            --query_plan_max_limit_for_lazy_materialization=0 \
            --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
            --query "$query"
    done
}

echo '-- simple ORDER BY ... LIMIT'
run_with_both_settings "SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 5"

echo '-- ORDER BY ... DESC LIMIT across files'
run_with_both_settings "SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k DESC LIMIT 7"

echo '-- with a filter (moved to PREWHERE)'
run_with_both_settings "SELECT k, s FROM ${TABLE_FN} WHERE f = 3 ORDER BY k DESC LIMIT 4"

echo '-- expressions on lazy columns are applied after the LIMIT'
run_with_both_settings "SELECT length(s) + f AS x, upper(s) FROM ${TABLE_FN} ORDER BY intDiv(k, 100) DESC, k LIMIT 3"

echo '-- sorting by a column with duplicate values'
run_with_both_settings "SELECT f, s FROM ${TABLE_FN} ORDER BY f, k LIMIT 5"

echo '-- virtual columns together with lazy columns'
run_with_both_settings "SELECT _file, _row_number, k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3 OFFSET 9998"

echo '-- the lazy read step is in the plan only when the optimization is enabled'
${CLICKHOUSE_CLIENT} \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3" | grep -c 'LazilyReadFromObjectStorage'
${CLICKHOUSE_CLIENT} \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=0 \
    --query "EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3" | grep -c 'LazilyReadFromObjectStorage' || true

echo '-- lazily read columns are shown in EXPLAIN'
${CLICKHOUSE_CLIENT} \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN actions = 1 SELECT k, s, arr FROM ${TABLE_FN} ORDER BY k LIMIT 3" | grep -o 'Lazily read columns: .*' | sort
