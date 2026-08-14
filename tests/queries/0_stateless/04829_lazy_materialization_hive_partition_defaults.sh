#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization on a hive-partitioned table, and its interaction with `DEFAULT` columns.
#
# Hive partition columns are parsed from the file path and appended to the chunk after the
# per-file reader pipeline, where `AddingDefaultsTransform` runs - so a `DEFAULT` expression never
# sees the real value of a hive partition column, on any plan: a query that has to evaluate such
# a default fails with `UNKNOWN_IDENTIFIER` with and without the optimization (a pre-existing
# limitation of hive partitioning, not introduced by lazy materialization; the same happens with
# the `File` engine). The planner keeps such defaulted columns on the main branch, which preserves
# exactly the single-pass behavior.
#
# A defaulted column whose expression only consumes columns read from the file is unaffected by
# hive partitioning: it is still deferred to the lazy branch together with the inputs of its
# expression, keeping the I/O reduction. The files carry only `k`, `b` and `s`; `p` comes from the
# path; `a DEFAULT b + 1` is missing from the files.
#
# The battery is run with the optimization enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_hive_default"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/p=1/data.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number * 2 AS b, concat('one_', toString(number)) AS s
    FROM numbers(0, 5)
    SETTINGS s3_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3('${URL}/p=2/data.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number * 3 AS b, concat('two_', toString(number)) AS s
    FROM numbers(5, 5)
    SETTINGS s3_truncate_on_insert = 1;
"

${CLICKHOUSE_CLIENT} --use_hive_partitioning=1 --query "
    DROP TABLE IF EXISTS lazy_mat_hive_default;
    DROP TABLE IF EXISTS lazy_mat_hive_path_default;
    CREATE TABLE lazy_mat_hive_default (k UInt64, p UInt64, b UInt64, a UInt64 DEFAULT b + 1, s String)
    ENGINE = S3('${URL}/p={1,2}/data.parquet', ${AUTH}, 'Parquet');
    CREATE TABLE lazy_mat_hive_path_default (k UInt64, p UInt64, b UInt64, a UInt64 DEFAULT p + b, s String)
    ENGINE = S3('${URL}/p={1,2}/data.parquet', ${AUTH}, 'Parquet');
"

QUERIES="
SELECT '-- a defaulted column with file-only inputs is deferred on a hive-partitioned table';
SELECT a, b, s, p FROM lazy_mat_hive_default ORDER BY k DESC LIMIT 3;
SELECT '-- the lazy read step is applied';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT a, b, s, p FROM lazy_mat_hive_default ORDER BY k DESC LIMIT 3);
SELECT '-- the defaulted column and the inputs of its expression are deferred; the hive column is not';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT a, b, s, p FROM lazy_mat_hive_default ORDER BY k DESC LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
# `s3_validate_etag_on_read` is pinned because for plain (non-data-lake) object storage the lazy
# reread is only generation-safe on `S3` with the ETag-pinned GET.
# `query_plan_max_limit_for_lazy_materialization` is pinned because the CI settings randomizer may
# lower it below the `LIMIT` of these queries.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_object_storage = $enabled"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --use_hive_partitioning=1 \
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "$QUERIES"

    echo "-- a default over a hive partition column cannot be evaluated on any plan (pre-existing)"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --use_hive_partitioning=1 \
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "SELECT a, b, s FROM lazy_mat_hive_path_default ORDER BY k LIMIT 3" 2>&1 \
        | grep -o "UNKNOWN_IDENTIFIER" | head -1
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE lazy_mat_hive_default;
    DROP TABLE lazy_mat_hive_path_default;
"
