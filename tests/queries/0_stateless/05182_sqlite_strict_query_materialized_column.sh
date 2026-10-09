#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_materialized.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t(i INTEGER NOT NULL, m INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (1, 2), (2, 3);
"

# A `MATERIALIZED` column of an external table is a physical column of the remote table: it is read from
# there, and a filter over it is pushed down like one over an ordinary column, so `external_table_strict_query`
# accepts it. An `ALIAS` column belongs to this source too, but it exists only locally: a filter over it is
# applied locally, and must be rejected under `external_table_strict_query` instead of being silently dropped
# as if it belonged to another table.
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext (i Int64, m Int64 MATERIALIZED i + 1, a Int64 ALIAS i * 10) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'materialized filter, default', count() FROM ext WHERE m = 2;

SELECT 'alias filter, default', count() FROM ext WHERE a = 10;

SELECT 'ordinary filter, strict', count() FROM ext WHERE i = 1
    SETTINGS external_table_strict_query = 1;
"

${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext (i Int64, m Int64 MATERIALIZED i + 1) ENGINE = SQLite('${DB_PATH}', 't');
SELECT 'materialized filter, strict', count() FROM ext WHERE m = 2
    SETTINGS external_table_strict_query = 1;
"

echo -n 'alias filter, strict: '
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext (i Int64, a Int64 ALIAS i * 10) ENGINE = SQLite('${DB_PATH}', 't');
SELECT count() FROM ext WHERE a = 10
    SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'
