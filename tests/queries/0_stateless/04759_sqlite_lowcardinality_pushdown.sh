#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A LowCardinality(...) column must not be pushdown-eligible: the storage read path routes every
# LowCardinality column through the text path (see SQLiteStatementReader), so the locally read value is
# SQLite's text rendering of the cell rather than the cell itself, and nothing pins that rendering to be
# exact (only SQLite 3.43+ renders a REAL cell with round-trip precision). A pushed-down predicate could
# therefore compare against a value the local path never sees and drop the row for good. The same column
# declared as a plain Float64 keeps its exact native accessor and stays pushdown-eligible.

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_lc_pushdown.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t (f REAL NOT NULL) STRICT;
INSERT INTO t VALUES (1.2345678901234567), (2.5);
"

${CLICKHOUSE_LOCAL} --query="
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lc (f LowCardinality(Float64)) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'A LowCardinality(Float64) filter is evaluated locally and keeps the full-precision row:';
SELECT f FROM lc WHERE f = 1.2345678901234567;
"

# The trace of the queries sent to SQLite proves what was pushed down: no WHERE for the LowCardinality
# column, the pushed-down filter for the same column declared as a plain Float64.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lc (f LowCardinality(Float64)) ENGINE = SQLite('${DB_PATH}', 't');
CREATE TABLE plain (f Float64) ENGINE = SQLite('${DB_PATH}', 't');
SELECT f FROM lc WHERE f = 2.5 FORMAT Null;
SELECT f FROM plain WHERE f = 2.5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `f` FROM `t`( WHERE .*)?$'

# Strict mode must reject a filter over the LowCardinality column: it cannot be fully evaluated by SQLite.
${CLICKHOUSE_LOCAL} --query="
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lc (f LowCardinality(Float64)) ENGINE = SQLite('${DB_PATH}', 't');
SET external_table_strict_query = 1;
SELECT f FROM lc WHERE f = 2.5;
" 2>&1 | grep -c 'INCORRECT_QUERY'
