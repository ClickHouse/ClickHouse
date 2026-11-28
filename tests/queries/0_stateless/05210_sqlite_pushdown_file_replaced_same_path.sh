#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pushdown decision of `ENGINE = SQLite` depends on remote metadata (a STRICT table, the declared column
# type, the text collation). That metadata must be read from the very database the scan runs against. The
# storage keeps a long-lived connection to the file it was first opened on; when the database file is replaced
# at the same path (an atomic `mv` of a freshly built file over it), that handle still sees the old, unlinked
# file, while each read opens the path anew and sees the replacement. Deriving the pushdown decision from the
# stale handle would reason about the old database (STRICT, BINARY collation - pushdown-safe) and query the new
# one (non-STRICT, TEXT-declared integer column, NOCASE collation - not pushdown-safe), and the predicate pushed
# down on the strength of the stale metadata would drop rows the local re-filtering never sees.

BASE="${USER_FILES_PATH}/05210_sqlite_replaced_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05210"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# The original file: a STRICT table whose columns are well matched, so both predicates are pushed down.
sqlite3 "${DB_PATH}" "
CREATE TABLE tbl (n INTEGER NOT NULL, s TEXT NOT NULL) STRICT;
INSERT INTO tbl VALUES (10, 'a'), (2, 'B');
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05210 (n Int64, s String) ENGINE = SQLite('${DB_PATH}', 'tbl')"

function run_queries()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT n FROM t_05210 WHERE n > 2 ORDER BY n"
    ${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05210 WHERE s > 'B' ORDER BY s"
}

# The query sent to SQLite is logged at trace level: with the predicate for a pushed-down filter, without it
# when the filter stays local.
function show_remote_queries()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT n FROM t_05210 WHERE n > 2 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT `[^`]*` FROM `tbl`( WHERE .*)?$'
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT s FROM t_05210 WHERE s > 'B' FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT `[^`]*` FROM `tbl`( WHERE .*)?$'
}

echo 'Original STRICT file, both predicates are pushed down and correct:'
run_queries
show_remote_queries

# Replace the database file at the same path. The storage has already opened its long-lived connection to the
# old file above. The replacement stores the integer column as TEXT ('10' < '2' lexicographically) and the text
# column with the NOCASE collation ('a' < 'B' case-folded, while ClickHouse compares 'a' > 'B' byte-wise), and
# the table is no longer STRICT, so neither predicate may be pushed down any more.
sqlite3 "${BASE}/new.sqlite" "
CREATE TABLE tbl (n TEXT NOT NULL, s TEXT COLLATE NOCASE NOT NULL);
INSERT INTO tbl VALUES ('10', 'a'), ('2', 'B');
"
mv "${BASE}/new.sqlite" "${DB_PATH}"

echo 'Replaced non-STRICT file, both predicates stay local and keep the rows:'
run_queries
show_remote_queries
