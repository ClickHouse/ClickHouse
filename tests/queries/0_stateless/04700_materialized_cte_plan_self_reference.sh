#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A materialized CTE's plan used to hold a strong reference back to the CTE that owns it, so a
# query dying inside `QueryPlan::optimize` - before `resolveMaterializingCTEs` claims the plan -
# leaked the whole graph together with the table references it carries. `DROP TABLE ... SYNC`
# then waited forever on `isSharedPtrUnique`.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04700 (uid Int16) ENGINE = MergeTree ORDER BY uid"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04700 VALUES (1)"

# `query_plan_max_optimizations_to_apply = 1` throws inside `QueryPlan::optimize`: after the CTE
# plan (and the cycle) is built, before the claim. Self-join keeps the CTE from being inlined.
$CLICKHOUSE_CLIENT --enable_materialized_cte=1 --query_plan_max_optimizations_to_apply=1 -q "
    WITH a AS MATERIALIZED (SELECT * FROM t_04700)
    SELECT count() FROM a AS l JOIN a AS r ON l.uid = r.uid
" 2>&1 | grep -oF 'TOO_MANY_QUERY_PLAN_OPTIMIZATIONS' | head -n 1

timeout 30 $CLICKHOUSE_CLIENT -q "DROP TABLE t_04700 SYNC" && echo "dropped" || echo "STUCK"
