#!/usr/bin/env bash
# A query scoped with SET ROLE must have that scope honored by remote reads over a cluster without
# an interserver secret, both for a Distributed table and for parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"
ROLE_NARROW="role_narrow_${CLICKHOUSE_DATABASE}"
ROLE_ADMIN="role_admin_${CLICKHOUSE_DATABASE}"

S_DIST="prefer_localhost_replica = 0, enable_parallel_replicas = 0, serialize_query_plan = 0"
S_PR="enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, serialize_query_plan = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0"
S_PR_DIST="enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, serialize_query_plan = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS logs_dist_nested;
DROP TABLE IF EXISTS logs_dist_pr;
DROP TABLE IF EXISTS logs_dist;
DROP TABLE IF EXISTS logs;
CREATE TABLE logs (svc String, x UInt32) ENGINE = MergeTree ORDER BY svc;
INSERT INTO logs SELECT 'narrow', number FROM numbers(100);
INSERT INTO logs SELECT 'secret', number FROM numbers(100);
CREATE TABLE logs_dist AS logs ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, logs);
CREATE TABLE logs_dist_pr AS logs ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, ${CLICKHOUSE_DATABASE}, logs);
CREATE TABLE logs_dist_nested AS logs ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, logs_dist);
"

$CLICKHOUSE_CLIENT -m -q "
DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
CREATE ROLE ${ROLE_NARROW};
CREATE ROLE ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs TO ${ROLE_NARROW}, ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs_dist TO ${ROLE_NARROW}, ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs_dist_pr TO ${ROLE_NARROW}, ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs_dist_nested TO ${ROLE_NARROW}, ${ROLE_ADMIN};
CREATE ROW POLICY p_narrow ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING svc = 'narrow' TO ${ROLE_NARROW};
CREATE ROW POLICY p_admin ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING 1 TO ${ROLE_ADMIN};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT ${ROLE_NARROW}, ${ROLE_ADMIN} TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ALL;
"

echo "-- narrow role, Distributed"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs_dist ORDER BY svc SETTINGS ${S_DIST};
"

echo "-- narrow role, parallel replicas"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs ORDER BY svc SETTINGS ${S_PR};
"

echo "-- narrow role, parallel replicas behind Distributed"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs_dist_pr ORDER BY svc SETTINGS ${S_PR_DIST};
"

echo "-- narrow role, nested Distributed"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs_dist_nested ORDER BY svc SETTINGS ${S_DIST};
"

$CLICKHOUSE_CLIENT -q "ALTER USER ${USER} DEFAULT ROLE ${ROLE_NARROW}"

echo "-- admin role over a narrow default role, Distributed"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_ADMIN};
SELECT DISTINCT svc FROM logs_dist ORDER BY svc SETTINGS ${S_DIST};
"

echo "-- admin role over a narrow default role, parallel replicas"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_ADMIN};
SELECT DISTINCT svc FROM logs ORDER BY svc SETTINGS ${S_PR};
"

echo "-- narrow role, local read"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs ORDER BY svc SETTINGS enable_parallel_replicas = 0;
"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_narrow ON ${CLICKHOUSE_DATABASE}.logs;
DROP ROW POLICY IF EXISTS p_admin ON ${CLICKHOUSE_DATABASE}.logs;
DROP TABLE IF EXISTS logs_dist_nested;
DROP TABLE IF EXISTS logs_dist_pr;
DROP TABLE IF EXISTS logs_dist;
DROP TABLE IF EXISTS logs;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
"
