#!/usr/bin/env bash
# Tags: atomic-database
# Tag atomic-database: a refreshable materialized view without APPEND and with a MergeTree target needs an Atomic database.

# The refresh of a `SQL SECURITY DEFINER` refreshable materialized view runs as the definer even when its query asks for
# parallel replicas: no replica can run the shipped read as the definer, so the refresh does not use parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

definer="definer05330_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

# `parallel_replicas_local_plan = 0` makes every replica a remote one, so a shipped read always runs.
pr_settings="enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0, parallel_replicas_mode = 'read_tasks', parallel_replicas_min_number_of_rows_per_replica = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $definer;
CREATE USER $definer;
CREATE TABLE $db.policy_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.policy_secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');
CREATE ROW POLICY policy05330 ON $db.policy_secrets USING owner = 'alice' TO $definer;
-- A refresh writes a new table and swaps it in.
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON $db.* TO $definer;
GRANT TABLE ENGINE ON MergeTree TO $definer;
CREATE MATERIALIZED VIEW $db.mv REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY secret EMPTY
    DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.policy_secrets SETTINGS $pr_settings;
EOF

${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW $db.mv; SYSTEM WAIT VIEW $db.mv" 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT secret FROM $db.mv ORDER BY secret"

# The same settings do read with parallel replicas when no view switches the user.
${CLICKHOUSE_CLIENT} --user "$definer" --query "SELECT secret FROM $db.policy_secrets SETTINGS $pr_settings, log_comment = '05330_pr_${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
      AND log_comment = '05330_pr_${CLICKHOUSE_DATABASE}'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv; DROP ROW POLICY policy05330 ON $db.policy_secrets; DROP USER $definer"
