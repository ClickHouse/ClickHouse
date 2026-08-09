#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL table engine, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The emulated `pg_catalog` is closed under its own discovery rules: every catalog view the
# PostgreSQL wire handler exposes has a named `pg_class` row and built-in `pg_attribute` rows, so a
# self-connected `postgresql(...)` resolves it like any other table and reads it back. The names are
# unqualified: PostgreSQL searches `pg_catalog` implicitly before the search path, and so does the
# emulated discovery. The `oid` columns are declared with the `oid` type (OID 26) and come back as
# `UInt32`, not `String`.

USER_NAME="pg_catalog_closure_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

SELECT '--- every emulated catalog view resolves and reads back through a self-connect';
SELECT 'pg_type', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_type', '${USER_NAME}', 'pgpass');
SELECT 'pg_attribute', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_attribute', '${USER_NAME}', 'pgpass');
SELECT 'pg_class', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_class', '${USER_NAME}', 'pgpass');
SELECT 'pg_namespace', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_namespace', '${USER_NAME}', 'pgpass');
SELECT 'pg_proc', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_proc', '${USER_NAME}', 'pgpass');
SELECT 'pg_range', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_range', '${USER_NAME}', 'pgpass');
SELECT 'pg_enum', count() > 0 FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_enum', '${USER_NAME}', 'pgpass');

SELECT '--- pg_type round-trips in full, including the leading oid column, and oid columns are numbers';
SELECT toTypeName(oid), toTypeName(typnamespace), toTypeName(typname)
FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_type', '${USER_NAME}', 'pgpass') LIMIT 1;
SELECT oid, typname, typelem
FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_type', '${USER_NAME}', 'pgpass')
WHERE typname IN ('oid', '_int4') ORDER BY oid;

SELECT '--- the catalog names resolve through pg_namespace to pg_catalog';
SELECT c.relname, c.relkind
FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_class', '${USER_NAME}', 'pgpass') AS c
INNER JOIN postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_namespace', '${USER_NAME}', 'pgpass') AS n
    ON c.relnamespace = n.oid
WHERE n.nspname = 'pg_catalog' AND c.relname != ''
ORDER BY c.relname;

DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
