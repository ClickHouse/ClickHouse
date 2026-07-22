#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Remote` database over a shard that points to this server reads and writes the underlying local
# tables directly, under the caller rather than under the stored engine credentials. Resolving a
# table of the database requires only `SHOW_COLUMNS` on the underlying table, which is not enough to
# read or write its data, so the caller's own `SELECT`/`INSERT` rights on the underlying table must
# be enforced (like the `remote` table function does for a local target).

REMOTE_DB="${CLICKHOUSE_DATABASE}_remote"
TEST_USER="user_04613_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1);
    CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', 'default', '');
    DROP USER IF EXISTS ${TEST_USER};
    CREATE USER ${TEST_USER} IDENTIFIED WITH no_password;
    GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER};
    GRANT SELECT, INSERT ON ${REMOTE_DB}.* TO ${TEST_USER};
"

echo '-- the table of the Remote database resolves for the user (SHOW COLUMNS is granted)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "DESCRIBE TABLE ${REMOTE_DB}.t" | cut -f1,2

echo '-- SELECT through it is rejected without SELECT on the underlying table (prints 1 if rejected)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT * FROM ${REMOTE_DB}.t" 2>&1 | grep -c -m1 "ACCESS_DENIED"

echo '-- INSERT through it is rejected without INSERT on the underlying table (prints 1 if rejected)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "INSERT INTO ${REMOTE_DB}.t VALUES (2)" 2>&1 | grep -c -m1 "ACCESS_DENIED"

echo '-- both work once the rights on the underlying table are granted'
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.t TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "INSERT INTO ${REMOTE_DB}.t VALUES (2)"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT * FROM ${REMOTE_DB}.t ORDER BY id"

echo '-- resolution is rejected without SHOW COLUMNS on the underlying local table (prints 1 if rejected)'
${CLICKHOUSE_CLIENT} --query "
    REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.* FROM ${TEST_USER};
    REVOKE SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.t FROM ${TEST_USER};
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "DESCRIBE TABLE ${REMOTE_DB}.t" 2>&1 | grep -c -m1 "ACCESS_DENIED"

echo '-- SHOW TABLES does not leak the underlying table name without SHOW TABLES on the underlying database (prints nothing)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SHOW TABLES FROM ${REMOTE_DB}"

echo '-- EXISTS TABLE does not leak the existence of the underlying table either (prints 0)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "EXISTS TABLE ${REMOTE_DB}.t"

echo '-- SHOW TABLES lists the table once SHOW TABLES on the underlying database is granted, even without SHOW COLUMNS'
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SHOW TABLES FROM ${REMOTE_DB}"

echo '-- EXISTS TABLE answers from the name only, so it works without SHOW COLUMNS as well (prints 1)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "EXISTS TABLE ${REMOTE_DB}.t"

echo '-- ...but DESCRIBE is still rejected without SHOW COLUMNS on the underlying table (prints 1 if rejected)'
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "DESCRIBE TABLE ${REMOTE_DB}.t" 2>&1 | grep -c -m1 "ACCESS_DENIED"

echo '-- a Remote database over another Remote database lists the names without resolving the structure'
# The outer database enumerates the inner (also `Remote`) one through the lightweight name-only
# iterator; going through the structure-resolving iterator instead would hide `t` here, because the
# user has no `SHOW COLUMNS` on the underlying table.
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${REMOTE_DB}_outer ENGINE = Remote('127.0.0.1', '${REMOTE_DB}', 'default', '');
    GRANT SHOW TABLES ON ${REMOTE_DB}_outer.* TO ${TEST_USER};
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SHOW TABLES FROM ${REMOTE_DB}_outer"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${REMOTE_DB}_outer"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${TEST_USER};
    DROP DATABASE ${REMOTE_DB};
    DROP TABLE ${CLICKHOUSE_DATABASE}.t;
"
