#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `COPY (query) TO STDOUT` is how libpq/pqxx stream result sets, so the inner query must see the same
# emulated catalog surface as a plain query: the `pg_catalog.` qualifier is stripped (otherwise the
# per-session catalog views are missed) and `pg_table_is_visible` is rewritten into the search-path check
# (otherwise the global function answers 1 for every relation).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05223_${CLICKHOUSE_DATABASE}"
OTHER_DATABASE="${CLICKHOUSE_DATABASE}_other_05223"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
DROP DATABASE IF EXISTS ${OTHER_DATABASE};

CREATE DATABASE ${OTHER_DATABASE};
CREATE TABLE ${CLICKHOUSE_DATABASE}.visible_here (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${OTHER_DATABASE}.only_in_other (x UInt32) ENGINE = MergeTree ORDER BY x;

CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
GRANT SELECT ON ${OTHER_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

echo '--- COPY (query) resolves the qualified catalog relations to the emulated views'
run_psql <<SQL
COPY (SELECT typname FROM pg_catalog.pg_type WHERE oid IN (23, 25) ORDER BY oid) TO STDOUT;
SQL

echo '--- COPY (query) and a plain query agree on pg_table_is_visible'
run_psql <<SQL
COPY (SELECT c.relname, pg_catalog.pg_table_is_visible(c.oid) FROM pg_catalog.pg_class AS c WHERE c.relname IN ('visible_here', 'only_in_other') ORDER BY c.relname) TO STDOUT;
SELECT c.relname, pg_catalog.pg_table_is_visible(c.oid) FROM pg_catalog.pg_class AS c WHERE c.relname IN ('visible_here', 'only_in_other') ORDER BY c.relname;
SQL

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${OTHER_DATABASE}.only_in_other;
DROP TABLE ${CLICKHOUSE_DATABASE}.visible_here;
DROP DATABASE ${OTHER_DATABASE};
DROP USER ${PG_USER};
"
