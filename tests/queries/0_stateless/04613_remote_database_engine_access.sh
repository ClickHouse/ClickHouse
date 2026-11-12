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

echo '-- a table the caller cannot see at all stays hidden: an existing and a missing name are'
echo '-- indistinguishable through resolution (prints UNKNOWN_TABLE for both, three times each)'
# Without this, the mere existence of a local table would be observable through the proxy: an
# existing name would be rejected by the `SHOW COLUMNS` check while a missing one falls through to
# "does not exist", letting a caller with rights only on the proxy database probe arbitrary names.
${CLICKHOUSE_CLIENT} --query "
    REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.* FROM ${TEST_USER};
    REVOKE SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.t FROM ${TEST_USER};
"
for name in t missing; do
    ${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "DESCRIBE TABLE ${REMOTE_DB}.${name}" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
    ${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SHOW CREATE TABLE ${REMOTE_DB}.${name}" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
    ${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT * FROM ${REMOTE_DB}.${name}" 2>&1 | grep -o -m1 "UNKNOWN_TABLE"
done

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

echo '-- ...and it needs no grants on the intermediate database, only on the tables it finally proxies'
# The listing of the inner `Remote` database is already filtered by the caller's rights on the tables
# it proxies, so the outer database must not additionally require `SHOW TABLES` on the name of the
# intermediate proxy: a user with no grants on it at all still sees `t` (prints `t`, then 1).
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${TEST_USER}_nested;
    CREATE USER ${TEST_USER}_nested IDENTIFIED WITH no_password;
    GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}_nested;
    GRANT SHOW TABLES ON ${REMOTE_DB}_outer.* TO ${TEST_USER}_nested;
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "SHOW TABLES FROM ${REMOTE_DB}_outer"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "EXISTS TABLE ${REMOTE_DB}_outer.t"

echo '-- ...but the tables invisible in the innermost database stay hidden (prints nothing, then 0)'
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* FROM ${TEST_USER}_nested"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "SHOW TABLES FROM ${REMOTE_DB}_outer"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "EXISTS TABLE ${REMOTE_DB}_outer.t"

echo '-- DESCRIBE through the chain needs no grants on the intermediate database either (prints id UInt64)'
# Resolving the table of the outer database resolves the table of the inner one, which has already
# checked `SHOW COLUMNS` against the object it finally proxies, so the outer database must not
# additionally require it on the name of the intermediate proxy: the rights on the outer database and
# on the innermost table are enough.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_nested (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t_nested VALUES (1), (2);
    GRANT SHOW COLUMNS, SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.t_nested TO ${TEST_USER}_nested;
    GRANT SELECT, INSERT ON ${REMOTE_DB}_outer.* TO ${TEST_USER}_nested;
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "DESCRIBE TABLE ${REMOTE_DB}_outer.t_nested" | cut -f1,2

echo '-- reading and writing the data, in contrast, needs the rights on every hop of the chain (prints 1, 1)'
# The query is really executed against the table of the intermediate database, exactly like for a
# `Distributed` table over another `Distributed` table, so the caller needs `SELECT` / `INSERT` on it.
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "SELECT * FROM ${REMOTE_DB}_outer.t_nested" 2>&1 | grep -c -m1 "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "INSERT INTO ${REMOTE_DB}_outer.t_nested VALUES (3)" 2>&1 | grep -c -m1 "ACCESS_DENIED"

echo '-- ...and then they work through the whole chain (prints 1, 2, 3)'
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${REMOTE_DB}.t_nested TO ${TEST_USER}_nested"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "INSERT INTO ${REMOTE_DB}_outer.t_nested VALUES (3)"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}_nested" --query "SELECT * FROM ${REMOTE_DB}_outer.t_nested ORDER BY id"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE ${CLICKHOUSE_DATABASE}.t_nested;
    DROP USER ${TEST_USER}_nested;
    DROP DATABASE ${REMOTE_DB}_outer;
"

echo '-- system.tables keeps the row of a table whose structure cannot be fetched, with an empty engine'
# The name has already been established by the listing, so a missing `SHOW COLUMNS` (or a transient
# `DESC TABLE` failure) must not make the table vanish from `system.tables`; the metadata columns
# that need the storage object stay empty instead.
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT name, engine FROM system.tables WHERE database = '${REMOTE_DB}'"

echo '-- a table of a multi-shard proxy resolves with the grants on the table alone (prints 4)'
# Two shards that both point to this server: reading the table must work with `SHOW COLUMNS` and
# `SELECT` on the underlying table alone, without any database-wide grant, and it reads both shards,
# so every row of the underlying table is counted twice.
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${REMOTE_DB}_sharded ENGINE = Remote('127.0.0.1,127.0.0.1', '${CLICKHOUSE_DATABASE}', 'default', '');
    REVOKE SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* FROM ${TEST_USER};
    GRANT SHOW COLUMNS, SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${TEST_USER};
    GRANT SELECT, SHOW TABLES ON ${REMOTE_DB}_sharded.* TO ${TEST_USER};
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT count() FROM ${REMOTE_DB}_sharded.t"

echo '-- ...and only the tables the caller may see are listed'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.hidden (id UInt64) ENGINE = MergeTree ORDER BY id;
"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SHOW TABLES FROM ${REMOTE_DB}_sharded"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE ${CLICKHOUSE_DATABASE}.hidden;
    DROP DATABASE ${REMOTE_DB}_sharded;
"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${TEST_USER};
    DROP DATABASE ${REMOTE_DB};
    DROP TABLE ${CLICKHOUSE_DATABASE}.t;
"
