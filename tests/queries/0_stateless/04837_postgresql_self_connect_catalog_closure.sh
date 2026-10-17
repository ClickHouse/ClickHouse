#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL table engine, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The emulated `pg_catalog` is closed under its own discovery rules: every catalog view the
# PostgreSQL wire handler exposes has a named `pg_class` row in the `pg_catalog` namespace and
# built-in `pg_attribute` rows, so a self-connected `postgresql(...)` resolves it like any other
# table and reads it back. The names are unqualified: PostgreSQL searches `pg_catalog` implicitly
# before the search path, and so does the emulated discovery. The `oid` columns are declared with
# the `oid` type (OID 26) and come back as `UInt32`, not `String`.
#
# The closure invariant is asserted over one `psql` connection, and only the read-back goes through
# `postgresql(...)`: every self-connect sets its own connection up (the catalog views are temporary,
# per connection), which is by far the most expensive part of this test.

USER_NAME="pg_catalog_closure_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

echo "--- every emulated catalog view has a pg_class row in pg_catalog and pg_attribute rows"
PGPASSWORD=pgpass psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" \
    --user "${USER_NAME}" --no-psqlrc --tuples-only --no-align <<'SQL' 2>&1
SELECT c.relname, c.relkind, n.nspname, count(a.attname) > 0
FROM pg_class AS c
INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
LEFT JOIN pg_attribute AS a ON a.attrelid = c.oid
WHERE c.relname IN ('pg_type', 'pg_attribute', 'pg_class', 'pg_namespace', 'pg_proc', 'pg_range', 'pg_enum')
GROUP BY c.relname, c.relkind, n.nspname
ORDER BY c.relname;
SQL

${CLICKHOUSE_CLIENT} -q "
SELECT '--- pg_type round-trips in full, including the leading oid column, and oid columns are numbers';
SELECT toTypeName(oid), toTypeName(typnamespace), toTypeName(typname), oid, typname, typelem
FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_type', '${USER_NAME}', 'pgpass')
WHERE typname IN ('oid', '_int4') ORDER BY oid;

SELECT '--- and so does pg_attribute, whose own rows describe the catalog views themselves';
SELECT count() > 0
FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'pg_attribute', '${USER_NAME}', 'pgpass')
WHERE attname = 'typname';

DROP USER ${USER_NAME};
"
