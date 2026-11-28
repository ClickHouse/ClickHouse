#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The standard introspection path of a PostgreSQL client is the catalog join
# `pg_attribute.atttypid = pg_type.oid` (not `format_type`), so every type OID the emulated
# `pg_attribute` can emit - scalars, `numeric`, `uuid`, and the array types - must have a row in the
# emulated `pg_type`, with `typelem` linking an array type to its element type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04757_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};

CREATE TABLE catalog_types
(
    n Decimal(20, 5),
    u UUID,
    an Array(Decimal(20, 5)),
    au Array(UUID),
    ai Array(Int32),
    s String
) ENGINE = Memory;
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- every column resolves through the pg_attribute -> pg_type join, arrays through typelem"
run_psql "
    SELECT a.attname, a.atttypid, t.typname, t.typcategory, et.typname AS elem
    FROM pg_attribute AS a
    JOIN pg_class AS c ON a.attrelid = c.oid
    JOIN pg_type AS t ON a.atttypid = t.oid
    LEFT JOIN pg_type AS et ON t.typelem = et.oid
    WHERE c.relname = 'catalog_types'
    ORDER BY a.attnum"

echo "--- no pg_attribute row is left without a pg_type row (built-in catalog rows included)"
run_psql "
    SELECT count()
    FROM pg_attribute AS a
    LEFT JOIN pg_type AS t ON a.atttypid = t.oid
    WHERE t.oid = 0"

echo "--- format_type agrees with the catalog on the oid type"
run_psql "SELECT format_type(26, -1)"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE catalog_types;
DROP USER ${PG_USER};
"
