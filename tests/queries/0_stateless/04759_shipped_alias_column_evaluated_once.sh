#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
drop table if exists pr_lazy;
drop table if exists pr_lazy_join;
create table pr_lazy (x UInt32, y Int64 ALIAS sleepEachRow(0.05)) engine = MergeTree order by x
    settings add_minmax_index_for_numeric_columns = 0;
insert into pr_lazy(x) select number from numbers(20);
create table pr_lazy_join (x UInt32) engine = MergeTree order by x;
insert into pr_lazy_join select number from numbers(20);
"

# An ALIAS column of a table expression shipped as a GLOBAL JOIN temporary table must be computed once
# per row, and the same number of times whichever way the query is dispatched. A larger count means it
# is computed both where the shipped side is materialized and again where the outer query reads it.
# Pinned to the query-based implementation: the plan-based one ships the right-hand side of the GLOBAL
# JOIN as a read of the base table instead of the temporary table already materialized for it, so the
# ALIAS is computed twice and this count is 40. Results are unaffected. Drop the pin once
# https://github.com/ClickHouse/ClickHouse/issues/116693 is fixed.
PR_SETTINGS="--enable_parallel_replicas=1 --max_parallel_replicas=3 \
--cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
--parallel_replicas_for_non_replicated_merge_tree=1 --automatic_parallel_replicas_mode=0 \
--parallel_replicas_plan_based=0"
QUERY="SELECT r.y FROM pr_lazy_join AS l GLOBAL INNER JOIN pr_lazy AS r ON l.x = r.x FORMAT Null"

# enable_analyzer is pinned on every arm, including the control: the shipping path under test only
# exists in the analyzer, and the arms must differ in the dispatch mode alone.
for mode in "--enable_parallel_replicas=0" "$PR_SETTINGS --parallel_replicas_local_plan=1" "$PR_SETTINGS --parallel_replicas_local_plan=0"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} --enable_analyzer=1 --profile-events-delay-ms=-1 --print-profile-events $mode --query "$QUERY" \
        |& grep -o -e "SleepFunctionCalls.*"
done

${CLICKHOUSE_CLIENT} --query "drop table pr_lazy_join; drop table pr_lazy"
