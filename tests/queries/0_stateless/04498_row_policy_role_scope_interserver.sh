#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# A query scoped with SET ROLE must have that scope honored on remote nodes too. Otherwise a remote
# node reading over an interserver-secret cluster falls back to the user's default roles and evaluates
# row policies against them, which for permissive policies can expose rows outside the active role.

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
CREATE TABLE logs_dist AS logs
    ENGINE = Distributed(test_cluster_interserver_secret, ${CLICKHOUSE_DATABASE}, logs, rand());
"

$CLICKHOUSE_CLIENT -m -q "
DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
CREATE ROLE ${ROLE_NARROW};
CREATE ROLE ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs TO ${ROLE_NARROW}, ${ROLE_ADMIN};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.logs_dist TO ${ROLE_NARROW}, ${ROLE_ADMIN};
CREATE ROW POLICY p_narrow ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING svc = 'narrow' TO ${ROLE_NARROW};
CREATE ROW POLICY p_admin ON ${CLICKHOUSE_DATABASE}.logs FOR SELECT USING 1 TO ${ROLE_ADMIN};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT ${ROLE_NARROW}, ${ROLE_ADMIN} TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ALL;
"

echo "-- narrow role, remote read: only 'narrow' must be visible"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_NARROW};
SELECT DISTINCT svc FROM logs_dist ORDER BY svc SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
"

echo "-- admin role, remote read: both rows must be visible (no over-restriction)"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE_ADMIN};
SELECT DISTINCT svc FROM logs_dist ORDER BY svc SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_narrow ON ${CLICKHOUSE_DATABASE}.logs;
DROP ROW POLICY IF EXISTS p_admin ON ${CLICKHOUSE_DATABASE}.logs;
DROP TABLE IF EXISTS logs_dist;
DROP TABLE IF EXISTS logs;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE_NARROW}, ${ROLE_ADMIN};
"
