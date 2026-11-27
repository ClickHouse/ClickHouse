#!/usr/bin/env bash
# Tags: no-fasttest
# On a secret interserver query the remote node enables the initiator's current roles as external roles and
# drops the user's default roles. The external roles must survive `Context::createCopy`: a copied context that
# recalculates access (here, a view with an access-related setting in its `SETTINGS` clause) must not lose them,
# otherwise a user whose grants come only from roles gets `ACCESS_DENIED` on the remote node.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"
ROLE="role_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(10);
CREATE VIEW v AS SELECT x FROM t SETTINGS allow_introspection_functions = 1;
CREATE TABLE t_dist AS t ENGINE = Distributed(test_cluster_interserver_secret, ${CLICKHOUSE_DATABASE}, t);
CREATE TABLE v_dist AS t ENGINE = Distributed(test_cluster_interserver_secret, ${CLICKHOUSE_DATABASE}, v);

DROP ROLE IF EXISTS ${ROLE};
CREATE ROLE ${ROLE};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${ROLE};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS readonly = 0;
GRANT ${ROLE} TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ${ROLE};
"

echo "-- remote read of a table"
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT sum(x) FROM t_dist SETTINGS prefer_localhost_replica = 0"

echo "-- remote read of a view with SETTINGS"
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT sum(x) FROM v_dist SETTINGS prefer_localhost_replica = 0"

echo "-- remote read of a view with SETTINGS, explicit SET ROLE"
$CLICKHOUSE_CLIENT --user "${USER}" -m -q "
SET ROLE ${ROLE};
SELECT sum(x) FROM v_dist SETTINGS prefer_localhost_replica = 0;
"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS v_dist;
DROP TABLE IF EXISTS t_dist;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE};
"
