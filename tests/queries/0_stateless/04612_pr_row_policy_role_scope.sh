#!/usr/bin/env bash
# Tags: no-fasttest
# A row policy scoped to a role must be honored on the parallel-replica read nodes: the initiator's
# ACTIVE role (SET ROLE) has to be propagated so the remote replicas evaluate the policy as that
# principal, not as their fallback/default identity. This is only possible over an interserver-secret
# cluster (identity is forwarded only in interserver mode) that also has several replicas per shard so
# parallel replicas actually engage.
# Related: PR #110867 (propagate current roles to remote read nodes), PR #110318 (row policy on the
# default parallel-replicas path).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"
ROLE_NARROW="role_narrow_${CLICKHOUSE_DATABASE}"
ROLE_ADMIN="role_admin_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS logs;
CREATE TABLE logs (svc String, x UInt32) ENGINE = MergeTree ORDER BY svc;
INSERT INTO logs SELECT 'narrow', number FROM numbers(100);
INSERT INTO logs SELECT 'secret', number FROM numbers(100);

DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
CREATE ROLE ${ROLE_NARROW};
CREATE ROLE ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs TO ${ROLE_NARROW}, ${ROLE_ADMIN};
DROP ROW POLICY IF EXISTS p_narrow, p_admin ON ${CLICKHOUSE_DATABASE}.logs;
CREATE ROW POLICY p_narrow ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING svc = 'narrow' TO ${ROLE_NARROW};
CREATE ROW POLICY p_admin  ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING 1          TO ${ROLE_ADMIN};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT ${ROLE_NARROW}, ${ROLE_ADMIN} TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ALL;
"

# enable_parallel_replicas = 2 -> throw if the parallel-replicas path cannot be used (no silent local
# fallback). parallel_replicas_local_plan = 0 and prefer_localhost_replica = 0 -> do all reading on the
# remote replicas: with a local plan the initiator reads the data itself (already under the active
# role) and the remote-side role propagation is never exercised. serialize_query_plan = 0 keeps the
# default (AST) path, on which each replica re-plans and re-applies its own row policy.
PR_SETTINGS="enable_analyzer = 1, serialize_query_plan = 0, automatic_parallel_replicas_mode = 0,
    enable_parallel_replicas = 2, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost_secret',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0, prefer_localhost_replica = 0"

echo "-- narrow role over parallel replicas: only 'narrow' visible"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs ORDER BY svc SETTINGS ${PR_SETTINGS}, log_comment = '04612_narrow_${CLICKHOUSE_DATABASE}';
"

echo "-- admin role over parallel replicas: both visible (no over-restriction)"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_ADMIN};
SELECT DISTINCT svc FROM logs ORDER BY svc SETTINGS ${PR_SETTINGS}, log_comment = '04612_admin_${CLICKHOUSE_DATABASE}';
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
echo "-- parallel replicas were actually used for both queries"
$CLICKHOUSE_CLIENT -q "
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - toIntervalMinute(30)
    AND type = 'QueryFinish'
    AND is_initial_query
    AND current_database = currentDatabase()
    AND log_comment IN ('04612_narrow_${CLICKHOUSE_DATABASE}', '04612_admin_${CLICKHOUSE_DATABASE}')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment
"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_narrow ON ${CLICKHOUSE_DATABASE}.logs;
DROP ROW POLICY IF EXISTS p_admin ON ${CLICKHOUSE_DATABASE}.logs;
DROP TABLE IF EXISTS logs;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
"
