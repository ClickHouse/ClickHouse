#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Plan-based parallel replicas (`parallel_replicas_plan_based = 1`) do not ship SQL text: the planner
# builds a plain local plan, `applyParallelReplicas` plants a split marker above every eligible
# `MergeTree` read, lifts it through the expression and filter steps above, and the fragment is
# serialized and executed on the other replicas under the connection's identity. The fence in
# `StorageView::readImpl` only keeps the *task-based* path out of the inner query of a
# `SQL SECURITY DEFINER` / `NONE` view that can hide rows; the plan-based pass has to stop at the
# security barrier steps itself, so that the view's inner read stays on the initiator and its
# filtering is never re-planned on a replica for a user who is not the definer.
#
# The plan of the invoker's query must not contain a remote parallel-replicas read for the view,
# while the same setup over a `SQL SECURITY INVOKER` view (the control) does distribute.

db=${CLICKHOUSE_DATABASE}
invoker="user05100_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05100_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.pb_pr_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.pb_pr_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.pb_pr_secrets SELECT 'someone_else', 'HIDDEN_' || toString(number) FROM numbers(3);

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.pb_pr_secrets TO $definer;
GRANT SELECT ON $db.pb_pr_secrets TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

CREATE ROW POLICY ${definer}_source_policy ON $db.pb_pr_secrets
FOR SELECT USING owner = 'visible_owner' TO $definer;

CREATE VIEW $db.pb_pr_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.pb_pr_secrets;

CREATE VIEW $db.pb_pr_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.pb_pr_secrets WHERE owner = 'visible_owner';

CREATE VIEW $db.pb_pr_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.pb_pr_secrets;

GRANT SELECT ON $db.pb_pr_definer_view TO $invoker;
GRANT SELECT ON $db.pb_pr_none_view TO $invoker;
GRANT SELECT ON $db.pb_pr_invoker_view TO $invoker;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 1 \
    --parallel_replicas_local_plan 1 --parallel_replicas_min_number_of_rows_per_replica 0 \
    --automatic_parallel_replicas_mode 0"

function remote_read_count()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "
        SELECT countIf(explain LIKE '%ReadFromParallelReplicas%')
        FROM (EXPLAIN optimize = 1, description = 0 $1)"
}

for view in pb_pr_definer_view pb_pr_none_view; do
    echo "--- $view ---"
    echo "remote reads in the plan: $(remote_read_count "SELECT owner, secret FROM $db.$view WHERE secret != ''")"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query \
        "SELECT * FROM $db.$view WHERE throwIf(secret LIKE 'HIDDEN%', 'LEAKED')" 2>&1 | grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo "leaked: 1" || echo "leaked: 0"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query \
        "SELECT owner, secret FROM $db.$view ORDER BY ALL"
done

echo "--- control: pb_pr_invoker_view ---"
echo "remote reads in the plan: $(remote_read_count "SELECT owner, secret FROM $db.pb_pr_invoker_view WHERE secret != ''")"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${definer}_source_policy ON $db.pb_pr_secrets"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.pb_pr_definer_view, $db.pb_pr_none_view, $db.pb_pr_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.pb_pr_secrets"
