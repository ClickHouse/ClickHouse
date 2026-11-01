#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL table engine, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The write side of the `postgresql` table function and the `PostgreSQL` engine pointed at this very server:
# the libpqxx sink streams the rows with `COPY ... FROM STDIN`, so quoting and `NULL` handling have to
# survive the round trip. The reading path is covered by `04665_postgresql_self_connect`.
#
# The `query(...)` variant works when the structure is given explicitly (the `PostgreSQL` engine with a column
# list), but inferring the structure of its result is not supported against a ClickHouse server yet: it is
# resolved with `unnest(...) WITH ORDINALITY AS t(...)`, which ClickHouse does not parse. This pins that
# limitation down, so the test has to be updated once the inference starts to work.

USER_NAME="pg_self_write_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE self_target (a UInt32, b String, c Nullable(Int64)) ENGINE = MergeTree ORDER BY a;

SELECT '--- INSERT through the postgresql table function';
INSERT INTO TABLE FUNCTION postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_target', '${USER_NAME}', 'pgpass')
VALUES (1, 'one', 10), (2, 'quote '' and \"double\" and \\\\backslash', NULL), (3, '', 30);
SELECT a, b, c FROM self_target ORDER BY a;

SELECT '--- INSERT through the PostgreSQL engine';
CREATE TABLE self_engine (a UInt32, b String, c Nullable(Int64))
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_target', '${USER_NAME}', 'pgpass');
INSERT INTO self_engine VALUES (4, 'tab\there', NULL);
SELECT a, b, c FROM self_target WHERE a = 4;
SELECT count() FROM self_engine;

DROP TABLE self_engine;
" | $CLICKHOUSE_CLIENT

echo "
SELECT '--- the query(...) variant with an explicit structure';
CREATE TABLE self_query_engine (a UInt32)
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', query('SELECT a FROM self_target WHERE c IS NULL'), '${USER_NAME}', 'pgpass');
SELECT a FROM self_query_engine ORDER BY a;
" | $CLICKHOUSE_CLIENT

echo "--- the query(...) variant without a structure is rejected"
$CLICKHOUSE_CLIENT --query "
SELECT * FROM postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', query('SELECT a FROM self_target'), '${USER_NAME}', 'pgpass')
" 2>&1 | grep -q -F 'Query execution failed' && echo "rejected"

echo "
DROP TABLE IF EXISTS self_query_engine;
DROP TABLE self_target;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
