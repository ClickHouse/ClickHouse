#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Cascades optimizer (`make_distributed_plan = 1, enable_cascades_optimizer = 1`) rebuilds the
# plan from its own memo and its rules neither see nor preserve the per-step security-barrier flag.
# It must stay off for a plan that contains a `SQL SECURITY DEFINER` / `NONE` view that may hide
# rows: the plan of such a query is the same with and without Cascades, while the `INVOKER` twin
# is rebuilt as usual.

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.t05098 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.t05098 SELECT number, number % 10 FROM numbers(1000);
CREATE VIEW $db.v05098_invoker SQL SECURITY INVOKER AS SELECT k, v FROM $db.t05098 WHERE k != 42;
CREATE VIEW $db.v05098_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, v FROM $db.t05098 WHERE k != 42;
CREATE VIEW $db.v05098_none SQL SECURITY NONE AS SELECT k, v FROM $db.t05098 WHERE k != 42;
EOF

CLIENT="${CLICKHOUSE_CLIENT} --make_distributed_plan 1 --distributed_plan_workers_num 1 --distributed_plan_execute_locally 1 --enable_analyzer 1 --enable_parallel_replicas 0 --explain_query_plan_default legacy --max_rows_to_group_by 0 --enable_join_runtime_filters 0"

function plan()
{
    # shellcheck disable=SC2086
    $CLIENT --enable_cascades_optimizer "$2" --query "EXPLAIN SELECT v, count() FROM $db.$1 GROUP BY v ORDER BY v"
}

for view in v05098_invoker v05098_definer v05098_none; do
    if diff <(plan $view 0) <(plan $view 1) > /dev/null; then
        echo "$view: same plan with and without Cascades"
    else
        echo "$view: different plan with and without Cascades"
    fi
    echo "$view result: $($CLIENT --enable_cascades_optimizer 1 --query "SELECT count(), sum(v), max(k) FROM $db.$view" 2>&1)"
done

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW $db.v05098_invoker;
DROP VIEW $db.v05098_definer;
DROP VIEW $db.v05098_none;
DROP TABLE $db.t05098;
EOF
