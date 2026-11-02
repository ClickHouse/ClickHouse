#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# Two properties of the emulated `pg_catalog` that a PostgreSQL client relies on:
#
# 1. `pg_table_is_visible` answers the search path. Every ClickHouse database is exposed as a schema,
#    so a relation of another database is reachable only through its schema and is not visible, while a
#    relation of the current database is - this is what psql's `\d` filters on.
# 2. The relation and namespace OID state (`pg_class_oids_data` / `pg_namespace_oids_data`) is filled
#    with the privileges of the session user. It is an ordinary session temporary table the client can
#    read directly, so an entry for a table the user has no `SHOW` privilege for would publish that
#    table's identity and defeat the grant filtering of the catalog views.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user names must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_05153_${CLICKHOUSE_DATABASE}"
PG_RESTRICTED_USER="postgresql_restricted_user_05153_${CLICKHOUSE_DATABASE}"
OTHER_DATABASE="${CLICKHOUSE_DATABASE}_other_05153"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
DROP USER IF EXISTS ${PG_RESTRICTED_USER};
DROP DATABASE IF EXISTS ${OTHER_DATABASE};

CREATE DATABASE ${OTHER_DATABASE};
CREATE TABLE ${CLICKHOUSE_DATABASE}.visible_here (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${OTHER_DATABASE}.only_in_other (x UInt32) ENGINE = MergeTree ORDER BY x;

CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
GRANT SELECT ON ${OTHER_DATABASE}.* TO ${PG_USER};

CREATE USER ${PG_RESTRICTED_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_RESTRICTED_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "$1" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

echo '--- a relation of the current database is visible, one of another database is not'
run_psql "${PG_USER}" <<SQL
SELECT c.relname, pg_catalog.pg_table_is_visible(c.oid)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE c.relname IN ('visible_here', 'only_in_other')
ORDER BY c.relname;
SQL

echo '--- a built-in catalog relation is always on the search path'
run_psql "${PG_USER}" <<SQL
SELECT pg_catalog.pg_table_is_visible(oid) FROM pg_catalog.pg_class WHERE relname = 'pg_class';
SQL

# The identity the OID state would key the hidden table and its database under. It is read with the
# privileges of the test user, which can see both, and looked up below as a literal - the restricted user
# cannot compute it, which is exactly the point.
HIDDEN_TABLE_IDENTITY=$(${CLICKHOUSE_CLIENT} -q "
SELECT if(uuid != toUUID('00000000-0000-0000-0000-000000000000'),
    concat('uuid:', toString(uuid)),
    concat('name:', hex(database), ':', hex(name)))
FROM system.tables WHERE database = '${OTHER_DATABASE}' AND name = 'only_in_other'")
HIDDEN_DATABASE_IDENTITY=$(${CLICKHOUSE_CLIENT} -q "
SELECT if(uuid != toUUID('00000000-0000-0000-0000-000000000000'),
    concat('uuid:', toString(uuid)),
    concat('name:', hex(name)))
FROM system.databases WHERE name = '${OTHER_DATABASE}'")

echo '--- the OID state holds no entry for a table the session user cannot see'
run_psql "${PG_RESTRICTED_USER}" <<SQL
SELECT count() FROM pg_catalog.pg_class WHERE relname = 'only_in_other';
SELECT count() FROM pg_class_oids_data WHERE identity = '${HIDDEN_TABLE_IDENTITY}';
SELECT count() FROM pg_namespace_oids_data WHERE identity = '${HIDDEN_DATABASE_IDENTITY}';
SELECT count() FROM pg_catalog.pg_class WHERE relname = 'visible_here';
SQL

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${OTHER_DATABASE}.only_in_other;
DROP TABLE ${CLICKHOUSE_DATABASE}.visible_here;
DROP DATABASE ${OTHER_DATABASE};
DROP USER ${PG_USER};
DROP USER ${PG_RESTRICTED_USER};
"
