#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04670_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets VALUES ('${user}', 'visible'), ('someone_else', 'HIDDEN');

-- Restricts which rows the invoker may see, so it must be a security boundary.
CREATE VIEW $db.filtering_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

CREATE VIEW $db.filtering_view_none
SQL SECURITY NONE
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

-- Hides no rows, so it must stay fully optimizable.
CREATE VIEW $db.projecting_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

-- The stored query projects every row; the policy itself is the security boundary.
CREATE VIEW $db.policy_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

-- A session additional-table-filter predicate is applied in the view's output namespace.
CREATE VIEW $db.additional_filter_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

-- The same view without a security context switch, as the optimization baseline.
CREATE VIEW $db.projecting_view_invoker
SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.secrets;

CREATE VIEW $db.invoker_view
SQL SECURITY INVOKER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.filtering_view TO $user;
GRANT SELECT ON $db.filtering_view_none TO $user;
GRANT SELECT ON $db.projecting_view TO $user;
GRANT SELECT ON $db.policy_view TO $user;
GRANT SELECT ON $db.additional_filter_view TO $user;
GRANT SELECT ON $db.invoker_view TO $user;
GRANT SELECT ON $db.projecting_view_invoker TO $user;
GRANT SELECT ON $db.secrets TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;

-- A policy on the view itself is also a security boundary, even though the stored query is only
-- a projection. It must not be inlined before the policy is discovered and applied.
CREATE ROW POLICY ${user}_view_policy ON $db.policy_view FOR SELECT USING owner = currentUser() TO $user;
EOF

echo "===== the view exposes one row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT secret FROM $db.filtering_view"

echo "===== an outer predicate cannot observe the filtered-out row ====="
# Without the barrier the outer WHERE and the view's WHERE are merged into one filter over the
# source table, and throwIf fires on a row the view is supposed to hide. `analyzer_inline_views`
# is a third way in: it replaces the view with its defining subquery before a plan even exists.
for view in filtering_view filtering_view_none policy_view; do
    for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
        # shellcheck disable=SC2086
        ${CLICKHOUSE_CLIENT} $settings --user "$user" --query \
            "SELECT * FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
            grep -c -F "LEAKED"
    done
done

echo "===== an additional filter on a view is a security boundary ====="
# The filter is attached to the view, rather than its base table, so both analyzers must retain
# the view as a separate subplan and keep the outer expression above the additional filter.
for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $settings --user "$user" --query \
        "SELECT * FROM $db.additional_filter_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')
         SETTINGS additional_table_filters = {'additional_filter_view': 'owner = currentUser()'}" 2>&1 |
        grep -c -F "LEAKED"
done

echo "===== nor through the value in a cast error message ====="
# A failing cast puts the offending value in the exception, so the oracle above becomes a plain
# read of the hidden row. The cast still fails on the invoker's own row; only 'HIDDEN' must not
# appear.
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT * FROM $db.filtering_view WHERE toUInt8(secret) = 1" 2>&1 | grep -c -F "HIDDEN"

echo "===== nor after the plan is serialized and optimized again on a shard ====="
# A shard receives the plan as bytes and optimizes it once more, so the barrier has to survive
# `QueryPlan::serialize`. The shard connects as the restricted user, so the view hides the same
# row there as it does locally.
for serialize in 1 0; do
    ${CLICKHOUSE_CLIENT} --query \
        "SELECT * FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', filtering_view, '$user', '')
         WHERE throwIf(secret = 'HIDDEN', 'LEAKED')
         SETTINGS serialize_query_plan = $serialize, enable_analyzer = 1" 2>&1 | grep -c -F "LEAKED"
done

# The checks below run as the restricted user so the row policy on `policy_view` applies. They
# pin every setting the shape depends on, because the test also runs with randomized settings. The
# settings are pinned on the client and not with a SETTINGS clause inside the subquery, because changing
# `enable_analyzer` in a subquery is rejected when the server default differs.
explain_client="${CLICKHOUSE_CLIENT} --user $user --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

echo "===== an outer predicate merges into a view that is not a barrier ====="
# One `Filter` step means the outer predicate and the view's own predicate ended up in the same
# filter, which is exactly what must not happen across a barrier.
for view in projecting_view invoker_view filtering_view filtering_view_none; do
    ${explain_client} --enable_analyzer 1 --query \
        "SELECT count() FROM (
             EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$view WHERE secret = 'x'
         ) WHERE explain ILIKE '%Filter column:%'"
done

echo "===== a projection-only view stays exactly as optimizable as an INVOKER view ====="
# On every path that substitutes the view into the outer query before a plan exists
# (`enable_analyzer = 0`, `analyzer_inline_views = 1`) as well as on the plan-level path, a
# `DEFINER` view that provably hides no rows must produce the very plan of the same view declared
# `SQL SECURITY INVOKER`, while a view that filters rows must not.
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    for pair in "projecting_view projecting_view_invoker" "filtering_view invoker_view"; do
        # shellcheck disable=SC2086
        set -- $pair
        if diff -q \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$1 WHERE secret = 'x'" 2>&1) \
            <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$2 WHERE secret = 'x'" 2>&1) > /dev/null
        then echo "same"; else echo "different"; fi
    done
done

prewhere_client="${CLICKHOUSE_CLIENT} --user $user --enable_analyzer 1 --enable_parallel_replicas 0
    --optimize_move_to_prewhere 1 --query_plan_optimize_prewhere 1
    --enable_multiple_prewhere_read_steps 1"

echo "===== a view that hides no rows keeps PREWHERE ====="
# Otherwise every DEFINER view would pay for the fix.
${prewhere_client} --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.projecting_view WHERE secret = 'x'
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== but a filtering DEFINER view keeps the outer predicate out of PREWHERE ====="
${prewhere_client} --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.filtering_view WHERE secret = 'x'
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== results through a barrier view are still correct ====="
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT secret FROM $db.filtering_view WHERE secret LIKE 'vis%'"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT count() FROM $db.filtering_view WHERE secret = 'HIDDEN'"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${user}_view_policy ON $db.policy_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $user"
