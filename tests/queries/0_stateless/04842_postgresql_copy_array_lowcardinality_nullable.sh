#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# An array whose elements are nullable round-trips through the PostgreSQL `COPY` sub-protocol,
# including a `NULL` element, also when the nullability is wrapped in `LowCardinality`: the writing
# side emits an unquoted `NULL` for a null element (a quoted `"NULL"` is the four-character string),
# and the reading side accepts it because `LowCardinality(Nullable(T))` holds nulls even though the
# column itself does not report as nullable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04842_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};

CREATE TABLE source (a Array(LowCardinality(Nullable(String))), b Array(Nullable(Int32))) ENGINE = Memory;
INSERT INTO source VALUES (['x', NULL, 'NULL', ''], [1, NULL, 3]), ([], []), ([NULL], [NULL]);
CREATE TABLE destination (a Array(LowCardinality(Nullable(String))), b Array(Nullable(Int32))) ENGINE = Memory;
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

echo "--- the literals a null element is written as"
run_psql <<'SQL'
COPY (SELECT a, b FROM source ORDER BY a, b) TO STDOUT;
SQL

echo "--- and they read back into the same values"
run_psql <<'SQL'
COPY destination FROM STDIN;
{"x",NULL,"NULL",""}	{"1",NULL,"3"}
{}	{}
{NULL}	{NULL}
\.
SQL

${CLICKHOUSE_CLIENT} -q "
SELECT toTypeName(a), a, b FROM destination ORDER BY a, b;
SELECT '--- the round trip is lossless', count() FROM destination AS d SEMI JOIN source AS s USING (a, b);
DROP TABLE destination;
DROP TABLE source;
DROP USER ${PG_USER};
"
