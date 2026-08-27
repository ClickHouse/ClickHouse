#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Task-based parallel replicas ship a query as SQL text to the other replicas, where it is
# re-planned under the connection's identity: the replicas apply the row policies of their
# connecting user and of the initial user (the invoker), but never of the view's definer. Without
# the fence in `StorageView::readImpl` the inner query of a `SQL SECURITY DEFINER` view engaged
# parallel replicas, the definer's row policy on the source table was dropped on the remote
# replicas, and the hidden rows came back through the union into the invoker's plan — observable
# both through the result and through an invoker `throwIf` above the barrier.
#
# `parallel_replicas_local_plan = 0` makes every replica (the local one included) read through a
# secondary query, which made the leak deterministic before the fix.

db=${CLICKHOUSE_DATABASE}
invoker="user05044_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05044_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.security_view_pr_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.security_view_pr_secrets VALUES ('visible_owner', 'visible'), ('someone_else', 'HIDDEN');

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.security_view_pr_secrets TO $definer;

CREATE VIEW $db.security_view_pr_view
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT * FROM $db.security_view_pr_secrets;
GRANT SELECT ON $db.security_view_pr_view TO $invoker;

CREATE ROW POLICY ${definer}_source_policy ON $db.security_view_pr_secrets
FOR SELECT USING owner = 'visible_owner' TO $definer;
EOF

PR_SETTINGS="--enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0"

for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS $settings --user "$invoker" --query \
        "SELECT * FROM $db.security_view_pr_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 | grep -c -F LEAKED
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS $settings --user "$invoker" --query \
        "SELECT owner, secret FROM $db.security_view_pr_view ORDER BY owner"
done

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${definer}_source_policy ON $db.security_view_pr_secrets"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_pr_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.security_view_pr_secrets"
