#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_generated_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# A SQLite table with a STORED and a VIRTUAL generated column. `table_xinfo` reports such columns
# (hidden = 3 / hidden = 2) and `SELECT *` returns them, so ClickHouse must keep them readable through
# the shared SQLite schema. But SQLite rejects explicit writes into generated columns, so the SQLite
# storage write path must omit them from the INSERT column list (otherwise an `INSERT INTO t (a, b, c)`
# fails with "cannot INSERT into generated column").
sqlite3 "$DB_PATH" "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED, c INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL);"

echo 'Schema exposes the generated columns for reads:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); DESCRIBE TABLE sqlite_gen.t;"

echo 'Insert through the SQLite storage path writes only base columns; SQLite computes the generated columns:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); INSERT INTO sqlite_gen.t (a) VALUES (1), (2); SELECT * FROM sqlite_gen.t ORDER BY a FORMAT TSVWithNames;"
