#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# A PostgreSQL client that probes for binary I/O support of a type does not only compare
# `pg_type.typreceive` against zero - it resolves the receive function through `pg_proc`. Every
# receive-function OID the emulated `pg_type` advertises must therefore have a `pg_proc` row, or
# such a client drops every emulated type; and no `pg_proc` receive row may be an orphan either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04850_${CLICKHOUSE_DATABASE}"

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

echo "--- the receive function of a representative type of every kind resolves through pg_proc"
run_psql "
    SELECT t.typname, t.typtype, t.typcategory, p.proname
    FROM pg_type AS t
    JOIN pg_proc AS p ON p.oid = t.typreceive
    WHERE t.typname IN ('bool', 'int4', 'text', 'numeric', 'timestamptz', '_int4', 'int8range', 'int8multirange', 'mood')
    ORDER BY t.typname"

echo "--- no type advertises a receive function that pg_proc does not resolve"
run_psql "
    SELECT count()
    FROM pg_type AS t
    LEFT JOIN pg_proc AS p ON p.oid = t.typreceive
    WHERE t.typreceive != 0 AND p.oid = 0"

echo "--- and every type advertises one: none is left at zero"
run_psql "SELECT count() FROM pg_type WHERE typreceive = 0"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
