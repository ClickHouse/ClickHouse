#!/usr/bin/env bash

# A view with `SQL SECURITY DEFINER` or `NONE` that hides rows is read through an opaque step,
# so the invoker's expressions and predicates never see the rows the view drops.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05257_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05257_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

# `parallel_replicas_local_plan = 0` makes every replica a remote one, so the shipped read always runs.
pr_settings="enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0, parallel_replicas_mode = 'read_tasks', parallel_replicas_min_number_of_rows_per_replica = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0"
# The cluster has an interserver secret, so a replica runs a shipped read as the initial user.
url_pr_settings="enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_interserver_secret', parallel_replicas_for_cluster_engines = 1, parallel_replicas_mode = 'read_tasks', parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user, $definer;
CREATE USER $user;
CREATE USER $definer;

CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');

CREATE VIEW $db.definer_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.none_view SQL SECURITY NONE AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.invoker_view SQL SECURITY INVOKER AS SELECT * FROM $db.secrets WHERE owner = 'alice';
CREATE VIEW $db.projection_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets;

-- A plain projection still hides rows if the definer has a row policy on the table.
CREATE TABLE $db.policy_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.policy_secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');
CREATE ROW POLICY policy05257 ON $db.policy_secrets USING owner = 'alice' TO $definer;
GRANT SELECT ON $db.policy_secrets TO $definer;
CREATE VIEW $db.policy_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.policy_secrets;
-- The view's own query asks for parallel replicas.
CREATE VIEW $db.pr_policy_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.policy_secrets SETTINGS $pr_settings;
GRANT READ ON URL, CREATE TEMPORARY TABLE ON *.* TO $definer;
CREATE VIEW $db.url_view DEFINER = $definer SQL SECURITY DEFINER
    AS SELECT x FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+42', 'TSV', 'x UInt8');

-- A NONE view applies no row policies, even when the invoker has one on the table.
CREATE TABLE $db.invoker_policy_secrets (owner String) ENGINE = MergeTree ORDER BY owner SETTINGS index_granularity = 1;
INSERT INTO $db.invoker_policy_secrets VALUES ('alice'), ('bob');
CREATE ROW POLICY invoker_policy05257 ON $db.invoker_policy_secrets USING owner = 'bob' TO $user;
CREATE VIEW $db.none_projection_view SQL SECURITY NONE AS SELECT owner FROM $db.invoker_policy_secrets;

-- So does a plain projection if the invoker has a row policy on the view itself.
CREATE VIEW $db.view_policy_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets;
CREATE ROW POLICY view_policy05257 ON $db.view_policy_view USING owner = 'alice' TO $user;

-- A parameterized view is sealed too.
CREATE VIEW $db.param_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM $db.secrets WHERE owner = {owner:String};

-- A Distributed table over a sealed view, and a sealed view over a Distributed table.
CREATE TABLE $db.dist_view AS $db.secrets ENGINE = Distributed(test_shard_localhost, $db, definer_view);
CREATE TABLE $db.dist_secrets AS $db.secrets ENGINE = Distributed(test_shard_localhost, $db, secrets);
CREATE VIEW $db.view_over_dist DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM $db.dist_secrets WHERE owner = 'alice';

GRANT SELECT ON $db.definer_view TO $user;
GRANT SELECT ON $db.param_view TO $user;
GRANT SELECT ON $db.none_view TO $user;
GRANT SELECT ON $db.policy_view TO $user;
GRANT SELECT ON $db.view_policy_view TO $user;
GRANT SELECT ON $db.pr_policy_view TO $user;
GRANT SELECT ON $db.none_projection_view TO $user;
GRANT SELECT ON $db.url_view TO $user;
EOF

echo "--- an outer expression is not evaluated on the hidden rows"
for view in definer_view none_view policy_view view_policy_view; do
    for inline in 0 1; do
        ${CLICKHOUSE_CLIENT} --user "$user" --analyzer_inline_views "$inline" --query "
            SELECT secret FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
    done
done

echo "--- nor on a remote server"
for serialize in 0 1; do
    ${CLICKHOUSE_CLIENT} --serialize_query_plan "$serialize" --query "
        SELECT secret FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', 'definer_view', '$user', '')
        WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
done

echo "--- nor through a \`Distributed\` table, with the plan shipped to the shard or built there"
for table in dist_view view_over_dist; do
    for serialize in 0 1; do
        ${CLICKHOUSE_CLIENT} --serialize_query_plan "$serialize" --prefer_localhost_replica 0 --query "
            SELECT secret FROM $db.$table WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
    done
done

echo "--- nor in a parameterized view"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT secret FROM $db.param_view(owner = 'alice') WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1

# `distributed_plan_max_rows_to_broadcast = 0` forces a bucketed read, so the plan of the tiny view has an exchange.
echo "--- nor in a distributed query plan, which is built for the view's plan on its own"
${CLICKHOUSE_CLIENT} --user "$user" --make_distributed_plan 1 --distributed_plan_execute_locally 1 --distributed_plan_max_rows_to_broadcast 0 --enable_parallel_replicas 0 --query "
    SELECT secret FROM $db.definer_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0" 2>&1
${CLICKHOUSE_CLIENT} --make_distributed_plan 1 --distributed_plan_execute_locally 1 --distributed_plan_max_rows_to_broadcast 0 --enable_parallel_replicas 0 --query "
    SELECT countIf(explain LIKE '%Exchange%') > 0 FROM (EXPLAIN SELECT secret FROM $db.definer_view WHERE secret = 'x')"

echo "--- nor on parallel replicas, which would run the view's reads as another user"
for allow_view in 0 1; do
    ${CLICKHOUSE_CLIENT} --user "$user" --query "
        SELECT secret FROM $db.policy_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0
        SETTINGS $pr_settings, parallel_replicas_allow_view_over_mergetree = $allow_view" 2>&1
done
for inline in 0 1; do
    ${CLICKHOUSE_CLIENT} --user "$user" --query "
        SELECT owner FROM $db.none_projection_view ORDER BY owner SETTINGS $pr_settings, analyzer_inline_views = $inline" 2>&1
done
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT secret FROM $db.pr_policy_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED') = 0 SETTINGS enable_parallel_replicas = 0" 2>&1
# The same settings do read with parallel replicas when no view switches the user.
${CLICKHOUSE_CLIENT} --user "$definer" --query "SELECT secret FROM $db.policy_secrets SETTINGS $pr_settings, log_comment = '05257_pr_${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
      AND log_comment = '05257_pr_${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT x FROM $db.url_view SETTINGS $url_pr_settings, log_comment = '05257_url_view_${CLICKHOUSE_DATABASE}'" 2>&1
${CLICKHOUSE_CLIENT} --user "$definer" --query "
    SELECT x FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+42', 'TSV', 'x UInt8')
    SETTINGS $url_pr_settings, log_comment = '05257_url_direct_${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(log_comment, '_' || currentDatabase(), ''), countIf(NOT is_initial_query) > 0 FROM system.query_log
    WHERE type = 'QueryFinish' AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
          AND log_comment IN ('05257_url_view_${CLICKHOUSE_DATABASE}', '05257_url_direct_${CLICKHOUSE_DATABASE}'))
    GROUP BY log_comment ORDER BY log_comment"

echo "--- an outer predicate does not skip data by the values of the hidden rows"
# The table is sorted by `secret`, so a predicate on it would skip granules by the primary key.
for view in definer_view policy_view view_policy_view; do
    for secret in HIDDEN nonexistent; do
        ${CLICKHOUSE_CLIENT} --user "$user" --use_query_condition_cache 0 --query_id "05257_${CLICKHOUSE_DATABASE}_${view}_$secret" --query "
            SELECT count() FROM $db.$view WHERE secret = '$secret'"
    done
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT query_id LIKE '%policy_view%', read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '05257_${CLICKHOUSE_DATABASE}_%'
    ORDER BY query_id"

echo "--- an outer ORDER BY ... LIMIT is not pushed into the view to stop reading early"
# Reading `secrets` in reverse order of `secret` would stop at the visible row before reaching the hidden one.
for view in definer_view none_view; do
    ${CLICKHOUSE_CLIENT} --user "$user" --use_query_condition_cache 0 --max_threads 1 --max_block_size 1 --query_id "05257order_${CLICKHOUSE_DATABASE}_${view}" --query "
        SELECT secret FROM $db.$view ORDER BY secret DESC LIMIT 1"
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '05257order_${CLICKHOUSE_DATABASE}_%'
    ORDER BY query_id"

echo "--- the view is still queried as usual"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT owner, secret FROM $db.definer_view ORDER BY secret LIMIT 1;
    SELECT count(), max(secret) FROM $db.definer_view GROUP BY owner;
    SELECT secret FROM $db.definer_view WHERE secret LIKE 'vis%';"

echo "--- a plan fragment that reads the view can be cloned"
# Without the in-memory buffer, the input of a correlated subquery is duplicated by cloning its plan.
${CLICKHOUSE_CLIENT} --query "
    SELECT secret FROM $db.definer_view AS v WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number = length(v.secret))
    SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 0"

echo "--- only a view that runs with other privileges and can hide rows is sealed"
for view in definer_view none_view invoker_view projection_view policy_view; do
    echo -n "$view: "
    ${CLICKHOUSE_CLIENT} --query "SELECT countIf(explain LIKE '%ReadFromSealedView%') FROM (EXPLAIN SELECT * FROM $db.$view WHERE secret = 'x')"
done

echo -n "param_view: "
${CLICKHOUSE_CLIENT} --query "SELECT countIf(explain LIKE '%ReadFromSealedView%') FROM (EXPLAIN SELECT * FROM $db.param_view(owner = 'alice') WHERE secret = 'x')"

echo -n "view_policy_view for the user with the policy: "
${CLICKHOUSE_CLIENT} --user "$user" --query "EXPLAIN SELECT * FROM $db.view_policy_view WHERE secret = 'x'" | grep -c ReadFromSealedView

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.policy_view; DROP VIEW $db.pr_policy_view; DROP VIEW $db.url_view; DROP ROW POLICY policy05257 ON $db.policy_secrets; DROP ROW POLICY invoker_policy05257 ON $db.invoker_policy_secrets; DROP ROW POLICY view_policy05257 ON $db.view_policy_view; DROP USER $user, $definer"
