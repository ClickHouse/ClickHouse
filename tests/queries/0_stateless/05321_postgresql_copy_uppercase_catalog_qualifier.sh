#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# PostgreSQL folds unquoted identifiers to lower case, so `COPY PG_CATALOG.PG_TYPE TO STDOUT` names the same
# relation as `COPY pg_catalog.pg_type TO STDOUT` and must reach the emulated catalog view. The same holds
# for a plain query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05321_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

lower=$(echo 'COPY pg_catalog.pg_type TO STDOUT;' | run_psql)
upper=$(echo 'COPY PG_CATALOG.PG_TYPE TO STDOUT;' | run_psql)
mixed=$(echo 'COPY Pg_Catalog.Pg_Type TO STDOUT;' | run_psql)

echo "$lower" | grep -q -P '^25\t11\ttext\t' && echo "lower case lists the catalog"
[ "$lower" = "$upper" ] && echo "upper case matches"
[ "$lower" = "$mixed" ] && echo "mixed case matches"

echo 'SELECT typname FROM PG_CATALOG.PG_TYPE WHERE oid = 25;' | run_psql

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
