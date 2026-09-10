#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Verify that push_external_roles_in_interserver_queries sends the correct current/granted roles.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROLE_A="role_a_${CLICKHOUSE_DATABASE}"
ROLE_B="role_b_${CLICKHOUSE_DATABASE}"
USER_ROLES="user_roles_${CLICKHOUSE_DATABASE}"
USER_NOROLES="user_noroles_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP TABLE IF EXISTS t_a;
    DROP TABLE IF EXISTS t_b;
    DROP TABLE IF EXISTS d_a;
    DROP TABLE IF EXISTS d_b;
    CREATE TABLE t_a (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_b (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_a VALUES (1);
    INSERT INTO t_b VALUES (2);
    CREATE TABLE d_a AS t_a ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), t_a, rand());
    CREATE TABLE d_b AS t_b ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), t_b, rand());

    DROP ROLE IF EXISTS ${ROLE_A}, ${ROLE_B};
    CREATE ROLE ${ROLE_A};
    CREATE ROLE ${ROLE_B};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_a TO ${ROLE_A};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_a TO ${ROLE_A};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_b TO ${ROLE_A};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_b TO ${ROLE_B};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_b TO ${ROLE_B};

    DROP USER IF EXISTS ${USER_ROLES};
    CREATE USER ${USER_ROLES} IDENTIFIED WITH no_password DEFAULT ROLE NONE;
    GRANT ${ROLE_A}, ${ROLE_B} TO ${USER_ROLES};

    DROP USER IF EXISTS ${USER_NOROLES};
    CREATE USER ${USER_NOROLES} IDENTIFIED WITH no_password DEFAULT ROLE NONE;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_a TO ${USER_NOROLES};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_a TO ${USER_NOROLES};
"

echo "-- role A, d_a"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_A};
    SELECT count() FROM d_a SETTINGS prefer_localhost_replica = 0;
"

echo "-- role B, d_b"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_B};
    SELECT count() FROM d_b SETTINGS prefer_localhost_replica = 0;
"

echo "-- role A on d_b (cross-role, denied on shard)"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_A};
    SELECT count() FROM d_b SETTINGS prefer_localhost_replica = 0;
" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo "-- role A+B, d_b"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_A}, ${ROLE_B};
    SELECT count() FROM d_b SETTINGS prefer_localhost_replica = 0;
"

echo "-- GLOBAL IN under role A"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_A};
    SELECT count() FROM d_a WHERE x GLOBAL IN (SELECT x FROM d_a) SETTINGS prefer_localhost_replica = 0;
"

echo "-- role switch in one session"
${CLICKHOUSE_CLIENT} --user "${USER_ROLES}" -m -q "
    SET ROLE ${ROLE_A};
    SELECT count() FROM d_a SETTINGS prefer_localhost_replica = 0;
    SET ROLE ${ROLE_B};
    SELECT count() FROM d_b SETTINGS prefer_localhost_replica = 0;
"

echo "-- no roles, direct grants"
${CLICKHOUSE_CLIENT} --user "${USER_NOROLES}" -q "
    SELECT count() FROM d_a SETTINGS prefer_localhost_replica = 0;
"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${USER_ROLES};
    DROP USER IF EXISTS ${USER_NOROLES};
    DROP ROLE IF EXISTS ${ROLE_A}, ${ROLE_B};
    DROP TABLE IF EXISTS d_a;
    DROP TABLE IF EXISTS d_b;
    DROP TABLE IF EXISTS t_a;
    DROP TABLE IF EXISTS t_b;
"
