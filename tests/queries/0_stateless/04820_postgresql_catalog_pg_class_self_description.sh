#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The emulated catalog must describe its own built-in relations: `pg_attribute` exposes column rows
# for `pg_type` (`attrelid = 1247`), so the standard client resolution `pg_class.relname ->
# pg_class.oid -> pg_attribute.attrelid` must find `pg_type` by name, under the `pg_catalog`
# namespace.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04820_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- pg_type resolves by name and lists its own columns"
run_psql "
    SELECT a.attname, a.attnum
    FROM pg_class AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    WHERE c.relname = 'pg_type'
    ORDER BY a.attnum"

echo "--- the built-in relations live in the pg_catalog namespace"
run_psql "
    SELECT c.relname, n.nspname, c.relkind
    FROM pg_class AS c
    JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE c.oid IN (1247, 1259)
    ORDER BY c.oid"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${PG_USER};
"
