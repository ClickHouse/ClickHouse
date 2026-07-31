#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the `postgresql` table function, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `postgresql` table function and the `PostgreSQL` table engine pointed at this very server, through
# its own PostgreSQL wire protocol port (issue #52639). ClickHouse acts as a libpq/pqxx client against
# itself: it introspects the emulated `pg_catalog` to discover the table structure and then streams the
# rows with `COPY ... TO STDOUT`.

USER_NAME="pg_self_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"
PG_SOURCE="postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass')"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

-- The emulated pg_namespace, pg_class and pg_attribute are views over these system tables, and a view is
-- expanded in the context of the user that reads it.
GRANT SELECT ON system.databases TO ${USER_NAME};
GRANT SELECT ON system.tables TO ${USER_NAME};
GRANT SELECT ON system.columns TO ${USER_NAME};
GRANT SELECT ON system.one TO ${USER_NAME};

CREATE TABLE self_source
(
    a UInt32,
    b String,
    c Nullable(Int64),
    d Decimal(18, 4),
    e Array(UInt8),
    f UInt64
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO self_source VALUES (1, 'one', 10, 1.5, [1, 2], 18446744073709551615), (2, 'two', NULL, -2.25, [], 0);
" | $CLICKHOUSE_CLIENT

echo "--- the query from the issue"
$CLICKHOUSE_CLIENT --query "SELECT 1 FROM postgresql('${PG_HOST}', 'system', 'one', '${USER_NAME}', 'pgpass')"

echo "--- reading a table back through the PostgreSQL protocol"
$CLICKHOUSE_CLIENT --query "SELECT a, b, c, d, e, f FROM ${PG_SOURCE} ORDER BY a"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${PG_SOURCE}"

echo "--- the structure schema inference recovers"
# Neither `UInt32` nor `UInt64` has a PostgreSQL counterpart: the first still fits `bigint`, the second is
# advertised as a `numeric` wide enough to hold every value.
$CLICKHOUSE_CLIENT --query "SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f) FROM ${PG_SOURCE} LIMIT 1"

echo "--- the same table through the PostgreSQL engine"
echo "
CREATE TABLE self_engine (a UInt32, b String)
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass');
SELECT a, b FROM self_engine ORDER BY a;
" | $CLICKHOUSE_CLIENT

echo "--- addressing the database as an explicit schema"
$CLICKHOUSE_CLIENT --query "SELECT a FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass', '${CLICKHOUSE_DATABASE}') ORDER BY a"

echo "--- current_setting('search_path') agrees with current_schema()"
# Unqualified names resolve in the connected database, so that is what the search path reports - and it
# has to report it as a PostgreSQL identifier, because a client parses the value as a comma-separated
# identifier list.
$CLICKHOUSE_CLIENT --query "SELECT current_setting('search_path') = currentDatabase(), current_schema() = currentDatabase()"

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

echo "
DROP TABLE self_engine;
DROP TABLE self_source;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
