#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The emulated `pg_catalog` must be closed under the standard type joins: every type OID that
# `pg_enum` (`enumtypid`) and `pg_range` (`rngtypid`, `rngmultitypid`, `rngsubtype`) expose must
# resolve through `pg_type.oid`, because a PostgreSQL client introspects enums and ranges with
# exactly these joins, and a missing `pg_type` row silently drops the type from the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04845_${CLICKHOUSE_DATABASE}"

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

echo "--- every enum label resolves through the pg_enum -> pg_type join"
run_psql "
    SELECT t.typname, t.typtype, t.typcategory, e.enumlabel
    FROM pg_enum AS e
    JOIN pg_type AS t ON e.enumtypid = t.oid
    ORDER BY e.enumsortorder"

echo "--- every range resolves through the pg_range -> pg_type joins: rngtypid, rngmultitypid and rngsubtype"
run_psql "
    SELECT t.typname, t.typtype, mt.typname, mt.typtype, st.typname
    FROM pg_range AS r
    JOIN pg_type AS t ON r.rngtypid = t.oid
    JOIN pg_type AS mt ON r.rngmultitypid = mt.oid
    JOIN pg_type AS st ON r.rngsubtype = st.oid
    ORDER BY t.typname"

echo "--- no pg_enum or pg_range type OID is left without a pg_type row"
run_psql "
    SELECT count()
    FROM
    (
        SELECT enumtypid AS oid FROM pg_enum
        UNION ALL SELECT rngtypid FROM pg_range
        UNION ALL SELECT rngmultitypid FROM pg_range
        UNION ALL SELECT rngsubtype FROM pg_range
    ) AS used
    LEFT JOIN pg_type AS t ON used.oid = t.oid
    WHERE t.oid = 0"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
