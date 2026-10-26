#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files over HTTP from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A web origin may omit `ETag`, `Content-Length`, and `Last-Modified` altogether, so the lazy
# reading pass could not prove that it rereads the same generation of the file and would fail
# close. `url` therefore must stay on the single-pass plan even when lazy materialization
# for object storage is enabled, and the query must keep working.

PATH_PREFIX="test/${CLICKHOUSE_DATABASE}_url_no_lazy_mat"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('http://localhost:11111/${PATH_PREFIX}/data.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

TABLE_FN="url('http://localhost:11111/${PATH_PREFIX}/data.parquet', 'Parquet')"

# `enable_analyzer` and `query_plan_max_limit_for_lazy_materialization` are pinned (and the
# optimization force-enabled) so the negative `EXPLAIN` check is not vacuous.
${CLICKHOUSE_CLIENT} \
    --enable_analyzer=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "
SELECT '-- no lazy step in the plan for url';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3);
SELECT '-- the query works';
SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3;
"
