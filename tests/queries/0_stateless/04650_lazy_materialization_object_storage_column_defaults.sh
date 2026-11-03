#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column that a Parquet file does not contain is computed from its `DEFAULT` expression by
# `AddingDefaultsTransform`, which runs inside every branch of the read pipeline and evaluates the
# expression over the columns of that branch alone: an input that the branch does not read is
# substituted with the type's default value. Lazy materialization must therefore never split a
# defaulted column away from the inputs of its expression - otherwise the expression would be
# computed from zeros, and with the defaulted column in the sort key the `LIMIT` would pick the
# wrong rows.
#
# The whole battery is run with the optimization enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_defaults"
AUTH="'test', 'testtest'"

# The files carry only `k`, `b` and `s`; `a` and `c` are missing and come from the defaults.
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
    DROP TABLE IF EXISTS lazy_mat_defaults;
    CREATE TABLE lazy_mat_defaults (k UInt64, b UInt64, a UInt64 DEFAULT b + 1, c UInt64 DEFAULT k * 10, s String)
    ENGINE = S3('${URL}/data_{1,2}.parquet', ${AUTH}, 'Parquet');
"

QUERIES="
SELECT '-- sorting by a defaulted column whose input is also selected';
SELECT b, s FROM lazy_mat_defaults ORDER BY a LIMIT 3;
SELECT '-- selecting a defaulted column together with its input';
SELECT a, b, s FROM lazy_mat_defaults ORDER BY k LIMIT 3;
SELECT '-- selecting a defaulted column whose input is the sorting key';
SELECT c, s FROM lazy_mat_defaults ORDER BY k LIMIT 3;
SELECT '-- the number of lazy read steps in the plan';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT b, s FROM lazy_mat_defaults ORDER BY a LIMIT 3);
SELECT '-- neither the defaulted columns nor the inputs of their expressions are deferred';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT b, s FROM lazy_mat_defaults ORDER BY a LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT c, s FROM lazy_mat_defaults ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- a defaulted column that the query does not read does not pin the inputs of its expression';
SELECT b, s FROM lazy_mat_defaults ORDER BY k LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT b, s FROM lazy_mat_defaults ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- a defaulted column needed only after the LIMIT is deferred with the inputs of its expression';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT a, b, s FROM lazy_mat_defaults ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- a defaulted column whose expression input stays on the main branch is not deferred';
SELECT a, s FROM lazy_mat_defaults ORDER BY b LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT a, s FROM lazy_mat_defaults ORDER BY b LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
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
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "$QUERIES"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE lazy_mat_defaults"
