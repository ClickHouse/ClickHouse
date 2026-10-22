#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A defaulted column consumed only by the PREWHERE / row-level filter is stripped from the step's
# output header, but the main branch of the lazy-materialization split still reads it and
# `AddingDefaultsTransform` evaluates its `DEFAULT` expression there before the filter runs.
# The inputs of that expression must therefore stay on the main branch even though neither the
# defaulted column nor its inputs appear in the step's output header: otherwise the expression
# would be computed from type defaults (zeros) and the filter would run on wrong values.
#
# Here the files carry only `k`, `b` and `s`; `a DEFAULT b + 1` is missing from the files and is
# used only as a filter input. If `b` were deferred, `a` would be computed as `0 + 1` for every
# row and the filter `a > 966` would select nothing.
#
# The battery is run with the optimization enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_filter_default"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data_1.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, 1000 - number * 7 AS b, concat('val_', toString(number)) AS s
    FROM numbers(0, 5)
    SETTINGS s3_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3('${URL}/data_2.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, 1000 - number * 7 AS b, concat('val_', toString(number)) AS s
    FROM numbers(5, 5)
    SETTINGS s3_truncate_on_insert = 1;
"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS lazy_mat_filter_default;
    CREATE TABLE lazy_mat_filter_default (k UInt64, b UInt64, a UInt64 DEFAULT b + 1, s String)
    ENGINE = S3('${URL}/data_{1,2}.parquet', ${AUTH}, 'Parquet');
"

QUERIES="
SELECT '-- filtering by a defaulted column that is not selected';
SELECT b, s FROM lazy_mat_filter_default WHERE a > 966 ORDER BY k LIMIT 3;
SELECT '-- the lazy read step is still applied (only s is deferred)';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT b, s FROM lazy_mat_filter_default WHERE a > 966 ORDER BY k LIMIT 3);
SELECT '-- the input of the filter-only default expression is not deferred';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT b, s FROM lazy_mat_filter_default WHERE a > 966 ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
# `s3_validate_etag_on_read` is pinned because for plain (non-data-lake) object storage the lazy
# reread is only generation-safe on `S3` with the ETag-pinned GET.
# `query_plan_max_limit_for_lazy_materialization` is pinned because the CI settings randomizer may
# lower it below the `LIMIT` of these queries.
# `query_plan_optimize_prewhere` and `optimize_move_to_prewhere` are pinned so that the filter on
# the defaulted column is actually pushed down into the read step.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_object_storage = $enabled"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_prewhere=1 \
        --optimize_move_to_prewhere=1 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "$QUERIES"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE lazy_mat_filter_default"
