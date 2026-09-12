#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `parallel_replicas_allow_view_over_mergetree = 1` lets query-based parallel replicas look through
# a "simple" view and read the `MergeTree` table below it instead of reading the view: the storage
# returned by `StorageView::getUnderlyingMergeTreeStorageForParallelReplicas` is announced and read
# under the *outer* query's context and identity. For a `SQL SECURITY DEFINER` / `NONE` view that
# hides rows, the view's filtering - the definer's row policy on the source table here - is not part
# of that read, so it reopens exactly the disclosure that `05059` fences off for the default `0`.
#
# The plan of the invoker's query must therefore carry no parallel-replicas read for a barrier view,
# and the rows the view hides must stay hidden; the same setup over a `SQL SECURITY INVOKER` view
# (the control) still distributes, which proves the setting is engaged for this view shape.

db=${CLICKHOUSE_DATABASE}
invoker="user05104_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05104_${CLICKHOUSE_DATABASE}_$RANDOM"

# The row policies are created after the views (see 04909). The test servers run with
# `throw_on_unmatched_row_policies`, so the invoker - who reads the source table directly through
# the `INVOKER` control - gets a pass-through policy of their own.
${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.vom_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.vom_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.vom_secrets SELECT 'someone_else', 'HIDDEN_' || toString(number) FROM numbers(3);

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.vom_secrets TO $definer;
GRANT SELECT ON $db.vom_secrets TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

CREATE VIEW $db.vom_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.vom_secrets;

CREATE VIEW $db.vom_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.vom_secrets WHERE owner = 'visible_owner';

CREATE VIEW $db.vom_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.vom_secrets;

GRANT SELECT ON $db.vom_definer_view TO $invoker;
GRANT SELECT ON $db.vom_none_view TO $invoker;
GRANT SELECT ON $db.vom_invoker_view TO $invoker;

CREATE ROW POLICY ${definer}_source_policy ON $db.vom_secrets
FOR SELECT USING owner = 'visible_owner' TO $definer;
CREATE ROW POLICY ${invoker}_source_policy ON $db.vom_secrets
FOR SELECT USING 1 TO $invoker;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 0 \
    --parallel_replicas_allow_view_over_mergetree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0 --automatic_parallel_replicas_mode 0"

function remote_read_count()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "
        SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%')
        FROM (EXPLAIN description = 0 $1)"
}

for view in vom_definer_view vom_none_view; do
    echo "--- $view ---"
    echo "remote reads in the plan: $(remote_read_count "SELECT owner, secret FROM $db.$view")"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query \
        "SELECT * FROM $db.$view WHERE throwIf(secret LIKE 'HIDDEN%', 'LEAKED')" 2>&1 | grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo "leaked: 1" || echo "leaked: 0"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query \
        "SELECT owner, secret FROM $db.$view ORDER BY ALL"
done

echo "--- control: vom_invoker_view ---"
echo "remote reads in the plan: $(remote_read_count "SELECT owner, secret FROM $db.vom_invoker_view")"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${definer}_source_policy, ${invoker}_source_policy ON $db.vom_secrets"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.vom_definer_view, $db.vom_none_view, $db.vom_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.vom_secrets"
