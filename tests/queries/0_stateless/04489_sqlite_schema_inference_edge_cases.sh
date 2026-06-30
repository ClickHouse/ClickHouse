#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GEN_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_edge_generated_XXXXXX.sqlite")
NULL_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_edge_nullable_XXXXXX.sqlite")
NUM_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_edge_numeric_XXXXXX.sqlite")
trap 'rm -f "$GEN_DB" "$NULL_DB" "$NUM_DB"' EXIT

# --- Generated columns must not be dropped from the inferred schema (they are visible to `SELECT *`) ---
sqlite3 "$GEN_DB" "CREATE TABLE t(a INTEGER NOT NULL, b INTEGER GENERATED ALWAYS AS (a + 1) STORED, c INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL);"
sqlite3 "$GEN_DB" "INSERT INTO t(a) VALUES (1), (2);"

echo 'Generated columns: schema'
${CLICKHOUSE_LOCAL} --query "DESC file('$GEN_DB', 'SQLite')" | cut -f1,2
echo 'Generated columns: data'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('$GEN_DB', 'SQLite') ORDER BY a"

# --- `schema_inference_make_columns_nullable` must be honored, and must be part of the schema cache key ---
sqlite3 "$NULL_DB" "CREATE TABLE t(id INTEGER NOT NULL, name TEXT);"
sqlite3 "$NULL_DB" "INSERT INTO t VALUES (1, 'x');"

${CLICKHOUSE_LOCAL} --multiquery --query "
    SET schema_inference_use_cache_for_file = 1;

    SELECT 'nullable=0';
    DESC file('$NULL_DB', 'SQLite') SETTINGS schema_inference_make_columns_nullable = 0;

    SELECT 'nullable=1';
    DESC file('$NULL_DB', 'SQLite') SETTINGS schema_inference_make_columns_nullable = 1;

    SELECT 'nullable=default (match metadata)';
    DESC file('$NULL_DB', 'SQLite');
" | cut -f1,2

# --- Native float reads must reject a BLOB instead of silently returning 0.0 ---
sqlite3 "$NUM_DB" "CREATE TABLE reals(x REAL);"
sqlite3 "$NUM_DB" "INSERT INTO reals VALUES (1.5), (2.5);"
sqlite3 "$NUM_DB" "CREATE TABLE ints(x INTEGER);"
sqlite3 "$NUM_DB" "INSERT INTO ints VALUES (42);"
sqlite3 "$NUM_DB" "CREATE TABLE blobs(x REAL);"
sqlite3 "$NUM_DB" "INSERT INTO blobs VALUES (x'0102');"

echo 'Float reads: REAL values'
${CLICKHOUSE_LOCAL} --query "SELECT x FROM file('$NUM_DB', 'SQLite', 'x Float64') ORDER BY x SETTINGS input_format_sqlite_table_name = 'reals'"
echo 'Float reads: INTEGER value as Float64'
${CLICKHOUSE_LOCAL} --query "SELECT x FROM file('$NUM_DB', 'SQLite', 'x Float64') SETTINGS input_format_sqlite_table_name = 'ints'"
echo 'Float reads: BLOB value rejected'
${CLICKHOUSE_LOCAL} --query "SELECT x FROM file('$NUM_DB', 'SQLite', 'x Float64') SETTINGS input_format_sqlite_table_name = 'blobs'" 2>&1 | grep -oF 'Cannot read a floating-point value' | head -1
