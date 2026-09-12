#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `StorageView::effectiveContextCanHideRows` enumerates the settings of a view's effective security
# context that hide rows on their own. The limits of `GROUP BY`, sorting and `DISTINCT` with a
# non-throwing overflow mode do hide rows - but only of a query that contains the corresponding
# operator, so they belong to `StorageView::shapeDependentOverflowCanHideRows`, which is applied
# once the shape of the query is known. Checked unconditionally, they turned every projection-only
# `SQL SECURITY DEFINER` view whose definer profile happens to carry, say,
# `group_by_overflow_mode = 'any'` into an optimization barrier, losing inlining, `PREWHERE`
# forwarding and the `ORDER BY ... LIMIT` pushdown for no security benefit at all.

db=${CLICKHOUSE_DATABASE}
invoker="user05184_${CLICKHOUSE_DATABASE}_$RANDOM"
group_definer="group05184_${CLICKHOUSE_DATABASE}_$RANDOM"
distinct_definer="distinct05184_${CLICKHOUSE_DATABASE}_$RANDOM"
sort_definer="sort05184_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.sho_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.sho_secrets VALUES ('a_owner', 'visible'), ('z_someone_else', 'other');

CREATE USER $invoker;
-- None of these can drop a row of a query that never aggregates, sorts or deduplicates.
CREATE USER $group_definer SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
CREATE USER $distinct_definer SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break';
CREATE USER $sort_definer SETTINGS max_rows_to_sort = 1, sort_overflow_mode = 'break';

GRANT SELECT ON $db.* TO $group_definer, $distinct_definer, $sort_definer, $invoker;

CREATE VIEW $db.sho_group_view DEFINER = $group_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.sho_secrets;
CREATE VIEW $db.sho_distinct_view DEFINER = $distinct_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.sho_secrets;
CREATE VIEW $db.sho_sort_view DEFINER = $sort_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.sho_secrets;
CREATE VIEW $db.sho_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.sho_secrets;

-- The same sort limit, but now the view's own query does sort, so the profile truncates its result.
CREATE VIEW $db.sho_sorted_view DEFINER = $sort_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.sho_secrets ORDER BY owner;
CREATE VIEW $db.sho_sorted_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.sho_secrets ORDER BY owner;
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

for view in sho_group_view sho_distinct_view sho_sort_view; do
    echo "===== a definer profile overflow limit of an operator the view does not use keeps it transparent: $view ====="
    for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
        # shellcheck disable=SC2086
        plans_the_same "$view" sho_invoker_view "$analyzer_settings"
    done
done

echo "===== the same sort limit over a view that does sort fails closed ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    plans_the_same sho_sorted_view sho_sorted_invoker_view "$analyzer_settings"
done

echo "===== results through the transparent views are correct ====="
for view in sho_group_view sho_distinct_view sho_sort_view; do
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query "SELECT secret FROM $db.$view ORDER BY ALL"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.sho_group_view, $db.sho_distinct_view, $db.sho_sort_view, $db.sho_invoker_view, $db.sho_sorted_view, $db.sho_sorted_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $group_definer, $distinct_definer, $sort_definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.sho_secrets"
