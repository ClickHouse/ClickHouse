#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_explicit_generated_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# A SQLite table with a generated column. Its `MATERIALIZED` (readable, non-insertable) classification comes
# from the remote SQLite schema, not from the ClickHouse metadata: a `MATERIALIZED` column without a default
# expression is formatted without the `MATERIALIZED` keyword, so an explicit `CREATE TABLE ... ENGINE = SQLite`
# and a `SHOW CREATE TABLE` round-trip both spell the generated column as an ordinary one. The storage
# re-consults the remote schema so the classification is preserved across these paths.
sqlite3 "$DB_PATH" "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"

# The explicit definition declares the generated column `b` as an ordinary column - exactly how a user would
# write it and exactly how `SHOW CREATE TABLE` round-trips it (see below).
DDL="CREATE TABLE t (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't')"

echo 'The explicitly declared generated column is kept:'
${CLICKHOUSE_LOCAL} --query "$DDL; DESCRIBE TABLE t;"

echo 'Insert without a column list, and an explicit base-column list, both target only the base column; SQLite computes the generated column:'
${CLICKHOUSE_LOCAL} --query "$DDL; INSERT INTO t VALUES (1); INSERT INTO t (a) VALUES (2); SELECT a, b FROM t ORDER BY a FORMAT TSVWithNames;"

echo 'The generated column is MATERIALIZED, so SELECT * returns only the base column:'
${CLICKHOUSE_LOCAL} --query "$DDL; SELECT * FROM t ORDER BY a FORMAT TSVWithNames;"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_LOCAL} --query "$DDL; INSERT INTO t (a, b) VALUES (3, 100);" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"

echo 'The rejected insert wrote nothing:'
${CLICKHOUSE_LOCAL} --query "$DDL; SELECT a, b FROM t ORDER BY a FORMAT TSVWithNames;"

echo 'SHOW CREATE TABLE round-trips the generated column as an ordinary column (no MATERIALIZED keyword):'
${CLICKHOUSE_LOCAL} --query "$DDL; SHOW CREATE TABLE t FORMAT TSVRaw" | grep -oE '`b` [A-Za-z0-9()]+( MATERIALIZED)?'

echo 'Recreating the table from that round-tripped definition keeps the generated column non-insertable:'
${CLICKHOUSE_LOCAL} --query "$DDL; INSERT INTO t (a, b) VALUES (4, 100);" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"
