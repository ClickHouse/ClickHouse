#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `StorageView::settingsClauseCanHideRows` is the AST-side proof that the `SETTINGS` clause of a
# view's own query cannot hide rows. The limits of `GROUP BY`, sorting and `DISTINCT` with a
# non-throwing overflow mode do hide rows - but only of a query that contains the corresponding
# operator, so, exactly like the same settings of a definer profile
# (`StorageView::shapeDependentOverflowCanHideRows`, test 05184), they are accepted in the clause
# of a query that provably lacks the operator. Rejected unconditionally, they turned a
# projection-only `SQL SECURITY DEFINER` view such as
# `SELECT owner, secret FROM t SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any'`
# into an optimization barrier, losing inlining, `PREWHERE` forwarding and the
# `ORDER BY ... LIMIT` pushdown for no security benefit at all.

db=${CLICKHOUSE_DATABASE}
invoker="user05212_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05212_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.shoc_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.shoc_secrets VALUES ('a_owner', 'visible'), ('z_someone_else', 'other');

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.* TO $definer, $invoker;

-- None of these clauses can drop a row of a query that never aggregates, sorts or deduplicates.
CREATE VIEW $db.shoc_group_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.shoc_secrets SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
CREATE VIEW $db.shoc_distinct_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.shoc_secrets SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break';
CREATE VIEW $db.shoc_sort_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.shoc_secrets SETTINGS max_rows_to_sort = 1, sort_overflow_mode = 'break';
CREATE VIEW $db.shoc_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.shoc_secrets;

-- The same sort limit, but now the view's own query does sort, so the clause truncates its result.
CREATE VIEW $db.shoc_sorted_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.shoc_secrets ORDER BY owner SETTINGS max_rows_to_sort = 1, sort_overflow_mode = 'break';
CREATE VIEW $db.shoc_sorted_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.shoc_secrets ORDER BY owner;

-- A row-hiding limit that does not depend on the shape of the query still fails closed.
CREATE VIEW $db.shoc_limit_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.shoc_secrets SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break';
EOSQL

explain_client="${CLICKHOUSE_CLIENT} --user $invoker --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

function plans_the_same()
{
    # shellcheck disable=SC2086
    if diff -q \
        <(${explain_client} $3 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$1 WHERE secret = 'x'" 2>&1) \
        <(${explain_client} $3 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$2 WHERE secret = 'x'" 2>&1) > /dev/null
    then echo "same"; else echo "different"; fi
}

for view in shoc_group_view shoc_distinct_view shoc_sort_view; do
    echo "===== a clause overflow limit of an operator the view does not use keeps it transparent: $view ====="
    for analyzer_settings in "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
        # shellcheck disable=SC2086
        plans_the_same "$view" shoc_invoker_view "$analyzer_settings"
    done
done

echo "===== the same sort limit over a view that does sort fails closed ====="
for analyzer_settings in "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    plans_the_same shoc_sorted_view shoc_sorted_invoker_view "$analyzer_settings"
done

echo "===== a shape-independent row-hiding setting in the clause fails closed ====="
for analyzer_settings in "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    plans_the_same shoc_limit_view shoc_invoker_view "$analyzer_settings"
done

echo "===== results through the transparent views are correct ====="
for view in shoc_group_view shoc_distinct_view shoc_sort_view; do
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query "SELECT secret FROM $db.$view ORDER BY ALL"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.shoc_group_view, $db.shoc_distinct_view, $db.shoc_sort_view, $db.shoc_invoker_view, $db.shoc_sorted_view, $db.shoc_sorted_invoker_view, $db.shoc_limit_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.shoc_secrets"
