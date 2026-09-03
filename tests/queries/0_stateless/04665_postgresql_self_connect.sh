#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the `postgresql` table function, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `postgresql` table function pointed at this very server, through its own PostgreSQL wire protocol
# port (issue #52639). ClickHouse acts as a libpq/pqxx client against itself: it introspects the emulated
# `pg_catalog` to discover the table structure and then streams the rows with `COPY ... TO STDOUT`.
#
# The `PostgreSQL` table engine and the schema handling live in `04692_postgresql_self_connect_engine`.
# Every self-connect opens a second connection and runs a handful of catalog queries on it, so each
# statement here is expensive under a sanitizer: keep the number of them, and of client invocations, low.

USER_NAME="pg_self_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"
PG_SOURCE="postgresql('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'self_source', '${USER_NAME}', 'pgpass')"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
-- Table-level access is all the self-connect needs: the emulated pg_namespace, pg_class and pg_attribute
-- are views over system.databases, system.tables and system.columns, which every user may read (their
-- rows are filtered by the user's own grants). The one below is for the table the first query reads.
GRANT SELECT ON system.one TO ${USER_NAME};

CREATE TABLE self_source
(
    a UInt32,
    b String,
    c Nullable(Int64),
    d Decimal(18, 4),
    e Array(UInt8),
    f UInt64,
    g Array(Nullable(Int32)),
    h Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO self_source VALUES (1, 'one', 10, 1.5, [1, 2], 18446744073709551615, [1, NULL], ['x', NULL]), (2, 'two', NULL, -2.25, [], 0, [], []);

SELECT '--- the query from the issue';
SELECT 1 FROM postgresql('${PG_HOST}', 'system', 'one', '${USER_NAME}', 'pgpass');

SELECT '--- reading a table back through the PostgreSQL protocol';
SELECT a, b, c, d, e, f FROM ${PG_SOURCE} ORDER BY a;

SELECT '--- the structure schema inference recovers';
SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f) FROM ${PG_SOURCE} LIMIT 1;

SELECT '--- nullable array elements survive schema inference';
SELECT has(g, NULL), has(h, NULL) FROM ${PG_SOURCE} ORDER BY a;

DROP TABLE self_source;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
