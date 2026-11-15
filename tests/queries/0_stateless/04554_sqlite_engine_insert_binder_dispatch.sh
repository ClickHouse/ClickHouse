#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# The `SQLite` storage engine / `sqlite` table function sink shares its value-binding dispatch with the
# `SQLite` output format: `Bool` is bound as SQLite INTEGER 0/1, `UInt64` (which can exceed the SQLite
# INTEGER range and would wrap through `sqlite3_bind_int64`) and NaN (which `sqlite3_bind_double` would turn
# into SQLite NULL, failing a NOT NULL column) are bound as text.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_insert_binder.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" 'CREATE TABLE t(u TEXT, b INTEGER, f REAL NOT NULL);'

${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 't') SELECT CAST(18446744073709551615 AS UInt64), true::Bool, nan::Float64"
${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 't') SELECT CAST(1 AS UInt64), false::Bool, 0.5::Float64"

echo 'Values and SQLite storage classes (UInt64 above INT64_MAX stays exact, Bool is INTEGER 0/1, NaN is text):'
sqlite3 "$DB_PATH" 'SELECT u, typeof(u), b, typeof(b), f, typeof(f) FROM t ORDER BY u;'

echo 'Read back through ClickHouse (u as String shows the full stored value):'
${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE r (u String, b Bool, f String) ENGINE = SQLite('${DB_PATH}', 't');
    SELECT u, b, f FROM r ORDER BY u;
"
