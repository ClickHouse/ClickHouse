#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `pg_attribute.attgenerated` is a standard PostgreSQL catalog field: a non-empty value means the column
# is generated. ClickHouse has no generated columns, so the emulated catalog must always report it empty,
# including for an `Array(Nullable(...))` column - the nullability of array *elements* is carried by the
# separate, emulation-only `attelemnotnull` column instead, and a self-connected `postgresql(...)` read
# still reconstructs `Array(Nullable(...))` from it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_05044_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};

CREATE TABLE t (plain Int32, arr Array(Nullable(String)), arr_not_null Array(String)) ENGINE = Memory;
INSERT INTO t VALUES (1, ['a', NULL], ['b']);
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align -c "$1" 2>&1
}

echo "--- no column of an ordinary table is reported as generated"
run_psql "
    SELECT a.attname, a.attgenerated = '' AS not_generated, a.attelemnotnull
    FROM pg_class AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    WHERE c.relname = 't'
    ORDER BY a.attnum"

echo "--- a query for generated columns finds none"
run_psql "
    SELECT count()
    FROM pg_class AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    WHERE c.relname = 't' AND a.attgenerated <> ''"

echo "--- element nullability still round-trips through a self-connect"
${CLICKHOUSE_CLIENT} -q "
DESCRIBE TABLE postgresql('127.0.0.1:${CLICKHOUSE_PORT_POSTGRESQL}', '${CLICKHOUSE_DATABASE}', 't', '${PG_USER}', '')
FORMAT TSV;
" | cut -f 1,2

${CLICKHOUSE_CLIENT} -q "
DROP USER ${PG_USER};
"
