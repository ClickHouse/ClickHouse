#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL table engine (built only with libpqxx) and postgresql-client

# The built-in `information_schema` database must be discoverable through the emulated PostgreSQL
# catalog. `pg_namespace` reports it under the fixed PostgreSQL OID and hides the generated row, so
# `pg_class.relnamespace` has to use that same fixed OID - otherwise resolving the schema by name and
# following `pg_namespace.oid -> pg_class.relnamespace` (what `fetchPostgreSQLTableStructure` does for
# a schema-qualified table) comes up empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04817_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
GRANT SELECT ON information_schema.* TO ${PG_USER};

CREATE TABLE is_probe (a UInt32) ENGINE = Memory;
"

function run_psql()
{
    PGPASSWORD=pgpass psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- the information_schema relations resolve through pg_namespace.oid -> pg_class.relnamespace"
run_psql "
    SELECT c.relname
    FROM pg_class AS c
    JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE n.nspname = 'information_schema' AND c.relname IN ('tables', 'columns')
    ORDER BY c.relname"

echo "--- a self-connect reads information_schema.columns as a schema-qualified table"
${CLICKHOUSE_CLIENT} -q "
SELECT column_name
FROM postgresql('${PG_HOST}', 'information_schema', 'columns', '${PG_USER}', 'pgpass')
WHERE table_schema = currentDatabase() AND table_name = 'is_probe';
"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE is_probe;
DROP USER ${PG_USER};
"
