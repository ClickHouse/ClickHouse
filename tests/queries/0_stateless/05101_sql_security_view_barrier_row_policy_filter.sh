#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy on a `SQL SECURITY DEFINER` / `NONE` view is the only thing that hides rows of a
# projection-only view. `StorageView` takes no `PREWHERE`, so both planners apply the policy as a
# `Filter` step above the view's subplan, and that step must itself be a security barrier: otherwise
# the invoker's `WHERE` merges into it and is evaluated on the rows the policy is about to drop.

user="user05101_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets VALUES ('${user}', 'visible'), ('someone_else', 'HIDDEN');

-- The stored query projects every row; the policy on the view is the only security boundary.
CREATE VIEW $db.policy_view_definer
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

CREATE VIEW $db.policy_view_none
SQL SECURITY NONE
AS SELECT owner, secret FROM $db.secrets;

-- The same view without a security context switch, as the optimization baseline.
CREATE VIEW $db.policy_view_invoker
SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.secrets;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.policy_view_definer TO $user;
GRANT SELECT ON $db.policy_view_none TO $user;
GRANT SELECT ON $db.policy_view_invoker TO $user;
GRANT SELECT ON $db.secrets TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;

CREATE ROW POLICY ${user}_definer ON $db.policy_view_definer FOR SELECT USING owner = currentUser() TO $user;
CREATE ROW POLICY ${user}_none ON $db.policy_view_none FOR SELECT USING owner = currentUser() TO $user;
CREATE ROW POLICY ${user}_invoker ON $db.policy_view_invoker FOR SELECT USING owner = currentUser() TO $user;
EOSQL

# The settings pin the plan shape the assertions below depend on, because the test also runs with
# randomized settings. They are given on the client and not in a SETTINGS clause, because changing
# `enable_analyzer` inside a subquery is rejected when the server default differs.
client="${CLICKHOUSE_CLIENT} --user $user --enable_parallel_replicas 0 --query_plan_merge_filters 1
    --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0 --query_plan_max_step_description_length 10000"

echo "===== the policy filter stays a separate step above a barrier view ====="
# A merged filter carries both descriptions joined with `+`; the policy filter of a barrier view
# must keep its own step, on both planners.
for analyzer in 1 0; do
    for view in policy_view_definer policy_view_none policy_view_invoker; do
        ${client} --enable_analyzer "$analyzer" --query \
            "SELECT '$view', countIf(explain LIKE '%Filter (Row-level security filter)%')
             FROM (EXPLAIN actions = 0, description = 1 SELECT * FROM $db.$view WHERE secret = 'x')"
    done
done

echo "===== an outer predicate cannot observe the row the policy hides ====="
# Without short-circuit evaluation every argument of the merged `and` is computed on every row, so a
# merged filter would run throwIf on the hidden row. The exception is matched by its code name and
# not by the message, because the client echoes the query text on failure.
for analyzer in 1 0; do
    for view in policy_view_definer policy_view_none; do
        ${client} --enable_analyzer "$analyzer" --short_circuit_function_evaluation disable --query \
            "SELECT * FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
            grep -c FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
    done
done

echo "===== results through the policy are still correct ====="
for analyzer in 1 0; do
    ${client} --enable_analyzer "$analyzer" --query \
        "SELECT secret FROM $db.policy_view_definer WHERE secret != 'x' ORDER BY secret"
done

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${user}_definer ON $db.policy_view_definer"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${user}_none ON $db.policy_view_none"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${user}_invoker ON $db.policy_view_invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $user"
