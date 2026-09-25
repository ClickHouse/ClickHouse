#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_generated_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# A SQLite table with a STORED and a VIRTUAL generated column. SQLite computes such columns itself and
# rejects explicit writes into them, so ClickHouse keeps them in the table structure but marks them
# MATERIALIZED: readable, but not insertable. As a result an insert without a column list targets only
# the base columns (matching SQLite), an explicit write into a generated column is rejected, and the
# generated columns are computed by SQLite rather than a user value being silently dropped.
sqlite3 "$DB_PATH" "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED, c INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL);"

echo 'Schema keeps the generated columns:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); DESCRIBE TABLE sqlite_gen.t;"

echo 'Insert without a column list, and an explicit base-column list, both target only the base columns; SQLite computes the generated columns:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); INSERT INTO sqlite_gen.t VALUES (1); INSERT INTO sqlite_gen.t (a) VALUES (2); SELECT a, b, c FROM sqlite_gen.t ORDER BY a FORMAT TSVWithNames;"

echo 'Generated columns are MATERIALIZED, so SELECT * returns only the base columns:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); SELECT * FROM sqlite_gen.t ORDER BY a FORMAT TSVWithNames;"

echo 'Explicitly writing into a generated column is rejected:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); INSERT INTO sqlite_gen.t (a, b) VALUES (3, 100);" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"

echo 'The rejected insert wrote nothing:'
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_gen ENGINE = SQLite('$DB_PATH'); SELECT a, b, c FROM sqlite_gen.t ORDER BY a FORMAT TSVWithNames;"
