#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# PostgreSQL folds every unquoted identifier to lower case, so an unqualified `PG_TYPE` names the `pg_type`
# catalog just like `pg_type` does, both in a plain query and in `COPY ... TO STDOUT`. A name qualified with
# another database keeps its case, because ClickHouse identifiers are case-sensitive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05325_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE ${CLICKHOUSE_DATABASE}.PG_TYPE (x String) ENGINE = Memory;
INSERT INTO ${CLICKHOUSE_DATABASE}.PG_TYPE VALUES ('own table');
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

echo 'SELECT typname FROM PG_TYPE WHERE oid = 25;' | run_psql
echo 'SELECT typname FROM Pg_Type WHERE oid = 25;' | run_psql

lower=$(echo 'COPY pg_type TO STDOUT;' | run_psql)
upper=$(echo 'COPY PG_TYPE TO STDOUT;' | run_psql)
echo "$lower" | grep -q -P '^25\t11\ttext\t' && echo "lower case lists the catalog"
[ "$lower" = "$upper" ] && echo "upper case matches"

echo "SELECT x FROM ${CLICKHOUSE_DATABASE}.PG_TYPE;" | run_psql

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}; DROP TABLE ${CLICKHOUSE_DATABASE}.PG_TYPE;"
