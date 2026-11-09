#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for object storage with requested JSON subcolumns: the format reads a
# subcolumn as its whole parent column and `ExtractColumnsTransform` extracts it afterwards, so
# deferring a subcolumn must bring its parent into the lazy branch's format header (and keep the
# parent in the main branch when a sort key still needs another subcolumn of it).

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_json"
AUTH="'test', 'testtest'"
STRUCTURE="'k UInt64, j JSON'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data.parquet', ${AUTH}, 'Parquet', ${STRUCTURE})
    SELECT number, toJSONString(map('user', map('name', concat('u', toString(number)), 'age', number)))
    FROM numbers(1000)
    SETTINGS s3_truncate_on_insert = 1;
"

TABLE_FN="s3('${URL}/data.parquet', ${AUTH}, 'Parquet', ${STRUCTURE})"

QUERIES="
SELECT '-- a JSON subcolumn is deferred through its parent column';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT k, j.user.name FROM ${TABLE_FN} ORDER BY k DESC LIMIT 3);
SELECT k, j.user.name FROM ${TABLE_FN} ORDER BY k DESC LIMIT 3;
SELECT '-- a deferred parent column whose subcolumn is a sort key input';
SELECT j FROM ${TABLE_FN} ORDER BY j.user.age.:Int64 DESC LIMIT 2;
"

# See 04612_parquet_lazy_materialization_object_storage for why the settings are pinned.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_object_storage = $enabled"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "$QUERIES"
done
