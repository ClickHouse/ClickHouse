#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# For plain (non-data-lake) object storage the object at a given path can be overwritten in place,
# so lazy materialization must reread the surviving files pinned to the exact generation the main
# pass read. Only `S3` with `s3_validate_etag_on_read` pins the actual GET (`If-Match` on the ETag);
# other backends open an unconditional read and could stitch together two versions of the file.
# Therefore the optimization is applied to a plain object storage only when that ETag-pinned reread
# is available: with `s3_validate_etag_on_read = 0` the plan must stay single-pass. In both cases the
# results must be identical.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_unpinned"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data_1.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(0, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION s3('${URL}/data_2.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(1000, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

TABLE_FN="s3('${URL}/data_{1,2}.parquet', ${AUTH}, 'Parquet')"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer, and
# `query_plan_max_limit_for_lazy_materialization = 0` because the CI settings randomizer may set it to 1.
for etag in 1 0; do
    echo "-- s3_validate_etag_on_read = $etag"
    echo "-- number of lazy read steps in the plan (1 only when the reread is ETag-pinned)"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --s3_validate_etag_on_read="$etag" \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --query "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3)"
    echo "-- results are the same regardless of the plan"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --s3_validate_etag_on_read="$etag" \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --query "SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3"
done
