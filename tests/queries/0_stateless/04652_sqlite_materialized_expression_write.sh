#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `SQLite` sink omits only the synthetic `MATERIALIZED` columns that stand for remote SQLite generated
# columns (SQLite computes those itself). A `MATERIALIZED` column the user declared with an expression is an
# ordinary column on the SQLite side, so its computed value must be written through.

DB_PATH="${CLICKHOUSE_TMP}/04652_sqlite.db"
STATE_PATH="${CLICKHOUSE_TMP}/04652_local_state"
rm -rf "${DB_PATH}" "${STATE_PATH}"

sqlite3 "${DB_PATH}" "
    CREATE TABLE plain (a INTEGER, b INTEGER);
    CREATE TABLE generated (a INTEGER, g INTEGER GENERATED ALWAYS AS (a * 10));
"

${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "
    CREATE DATABASE db_04652 ENGINE = Atomic;
    CREATE TABLE db_04652.plain (a Int64, b Int64 MATERIALIZED a + 1) ENGINE = SQLite('${DB_PATH}', 'plain');
    CREATE TABLE db_04652.generated (a Int64, g Int64) ENGINE = SQLite('${DB_PATH}', 'generated');

    INSERT INTO db_04652.plain (a) VALUES (5);
    INSERT INTO db_04652.generated (a) VALUES (7);

    SELECT 'A local MATERIALIZED expression is written to SQLite:';
    SELECT a, b FROM db_04652.plain ORDER BY a;

    SELECT 'A remote generated column is computed by SQLite:';
    SELECT a, g FROM db_04652.generated ORDER BY a;
"

echo "The same values as seen by SQLite itself:"
sqlite3 "${DB_PATH}" "SELECT a, b FROM plain; SELECT a, g FROM generated;"

rm -rf "${DB_PATH}" "${STATE_PATH}"
