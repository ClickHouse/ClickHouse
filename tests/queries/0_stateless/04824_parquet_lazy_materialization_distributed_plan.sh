#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# - no-fasttest: reads Parquet files from Minio
# - no-old-analyzer: make_distributed_plan and lazy materialization require the analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization must not fire while the initiator builds a distributed plan
# (`make_distributed_plan`): `JoinLazyColumnsStep` / `LazilyReadFromObjectStorage` are not
# serializable for remote execution, so a fragment containing them would be rejected by
# `convertToDistributed` with `SUPPORT_IS_DISABLED`. The protection is structural: the distributed
# sorting rewrite (`tryMakeDistributedSorting`) splits every full sort into a local top-N below a
# `GatherExchange` earlier in the same optimization pass, and the lazy pass does not descend below
# a logical exchange (a lazy branch must stay within one fragment). This test pins that ordering:
# with `make_distributed_plan = 1` the plan must contain the exchange split and no lazy read step,
# and the distributed conversion must succeed; with `make_distributed_plan = 0` the same query must
# keep taking the lazy path.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_mat_distributed"
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

# `query_plan_max_limit_for_lazy_materialization = 0` because the CI settings randomizer may set it
# to 1, and `s3_validate_etag_on_read = 1` because the lazy plan requires the ETag-pinned reread.
SETTINGS_COMMON=(
    --enable_analyzer=1
    --s3_validate_etag_on_read=1
    --query_plan_optimize_lazy_materialization=1
    --query_plan_max_limit_for_lazy_materialization=0
    --query_plan_optimize_lazy_materialization_for_object_storage=1
)

echo "-- make_distributed_plan = 0: the lazy read step is present"
${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" --make_distributed_plan=0 \
    --query "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3)"
${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" --make_distributed_plan=0 \
    --query "SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3"

# `make_distributed_plan` is set on the inner query only: on the outer query it would try to
# distribute the read from the EXPLAIN subquery itself, which is not a serializable step.
echo "-- make_distributed_plan = 1: the sort is exchange-split, no lazy read step in the plan"
${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" \
    --query "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%'), countIf(explain LIKE '%GatherExchange%') >= 1 FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3 SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0)"

echo "-- make_distributed_plan = 1: the distributed conversion succeeds (every fragment is serializable)"
${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" \
    --query "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN distributed = 1 SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3 SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0)"
