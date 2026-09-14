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

# `ReadFromObjectStorageStep` is serializable only in the private build, so the two builds take
# different routes for the same query and the expectations differ per build:
#   * private: the read can ship, so the plan is distributed - `tryMakeDistributedSorting` splits
#     the sort under a `GatherExchange` and the lazy pass does not descend below it, so the plan
#     carries the exchange and no lazy read step;
#   * open-source: the read cannot ship, so the pre-pass decision falls back to local execution -
#     the plan carries no exchange, and lazy materialization applies to it as to any local plan.
# Either way the invariant holds: a lazy read step never sits in a plan that gets shipped.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# `make_distributed_plan` is set on the inner query only: on the outer query it would try to
# distribute the read from the EXPLAIN subquery itself, which is not a serializable step.
plan_shape() # $1: EXPLAIN prefix
{
    ${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" \
        --query "SELECT concat('lazy_materialization:', toString(countIf(explain LIKE '%LazilyReadFromObjectStorage%') > 0)),
                        concat('distributed_exchanges:', toString(countIf(explain LIKE '%GatherExchange%') > 0))
                 FROM ($1 SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3 SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0)"
}

check_plan_shape() # $1: label, $2: EXPLAIN prefix
{
    local expected shape
    if [ "$IS_CLOUD" = 1 ]; then
        expected=$(printf 'lazy_materialization:0\tdistributed_exchanges:1')
    else
        expected=$(printf 'lazy_materialization:1\tdistributed_exchanges:0')
    fi
    shape=$(plan_shape "$2")
    [ "$shape" = "$expected" ] && echo "$1: ok" || echo "$1: FAIL: expected '$expected', got '$shape'"
}

echo "-- make_distributed_plan = 1: lazy read step and exchanges never coexist"
check_plan_shape "plan" "EXPLAIN"
check_plan_shape "distributed plan" "EXPLAIN distributed = 1"

# Executing it covers what the plan greps cannot: distributed execution in the private build, the
# fallback path in the open-source one. The rows must match the non-distributed query above.
echo "-- make_distributed_plan = 1: the query runs and returns the same rows"
${CLICKHOUSE_CLIENT} "${SETTINGS_COMMON[@]}" \
    --query "SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3 SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0"
