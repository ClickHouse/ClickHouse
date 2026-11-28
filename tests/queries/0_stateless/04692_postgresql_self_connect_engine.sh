#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL table engine, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `PostgreSQL` table engine pointed at this very server, and the schema half of the emulated
# `pg_catalog`: a ClickHouse database is a schema, and the one unqualified names resolve in is reported
# by `current_schema()` and `current_setting('search_path')`. The reading path itself is covered by
# `04665_postgresql_self_connect`.

USER_NAME="pg_self_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE self_source (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO self_source VALUES (1, 'one'), (2, 'two');

SELECT '--- the table through the PostgreSQL engine';
CREATE TABLE self_engine (a UInt32, b String)
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass');
SELECT a, b FROM self_engine ORDER BY a;

SELECT '--- addressing the database as an explicit schema';
SELECT a FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass', '${CLICKHOUSE_DATABASE}') ORDER BY a;

SELECT '--- the search path agrees with current_schema()';
SELECT current_setting('search_path') = currentDatabase(), current_schema() = currentDatabase();

DROP TABLE self_engine;
DROP TABLE self_source;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT

# A name that PostgreSQL would down-case, or split on the comma, has to be quoted, with an embedded double
# quote doubled - otherwise a client resolving unqualified names through the search path would end up in a
# different schema than the one the server itself uses.
WEIRD_DB="${CLICKHOUSE_DATABASE} Mixed,\"Case"
echo "
CREATE DATABASE \`${WEIRD_DB}\`;
USE \`${WEIRD_DB}\`;
SELECT replaceAll(current_setting('search_path'), '${CLICKHOUSE_DATABASE}', 'db');
" | $CLICKHOUSE_CLIENT
$CLICKHOUSE_CLIENT --query "DROP DATABASE \`${WEIRD_DB}\`"
