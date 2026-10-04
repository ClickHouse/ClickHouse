#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pushdown-safety check of `ENGINE = SQLite` probes the remote metadata, so it only runs for the columns
# that the `WHERE` refers to. The decision must still be right for a filtered column that is not read and for
# a column that is only one branch of a disjunction, and a query without a filter must not be affected at all.

BASE="${USER_FILES_PATH}/05260_sqlite_filter_columns_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05260"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# `n` is pushdown-safe (STRICT, INTEGER, NOT NULL, read as `Int64`); `u` is not (read as `UInt8`, which
# truncates the INTEGER cell 300 to 44).
sqlite3 "${DB_PATH}" "
CREATE TABLE tbl (n INTEGER NOT NULL, u INTEGER NOT NULL, s TEXT NOT NULL) STRICT;
INSERT INTO tbl VALUES (1, 300, 'a'), (2, 44, 'b'), (3, 5, 'c');
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05260 (n Int64, u UInt8, s String) ENGINE = SQLite('${DB_PATH}', 'tbl')"

function remote_query()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "$1 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tbl`( WHERE .*)?$'
}

echo 'No filter:'
${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05260 ORDER BY s"
remote_query "SELECT s FROM t_05260"

echo 'Safe filter on a column that is not read:'
${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05260 WHERE n > 1 ORDER BY s"
remote_query "SELECT s FROM t_05260 WHERE n > 1"

echo 'Unsafe filter on a column that is not read stays local:'
${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05260 WHERE u = 44 ORDER BY s"
remote_query "SELECT s FROM t_05260 WHERE u = 44"

echo 'Disjunction with an unsafe branch stays local as a whole:'
${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05260 WHERE n = 3 OR u = 44 ORDER BY s"
remote_query "SELECT s FROM t_05260 WHERE n = 3 OR u = 44"

echo 'Strict mode:'
${CLICKHOUSE_CLIENT} --external_table_strict_query 1 --query "SELECT s FROM t_05260 WHERE n > 1 ORDER BY s"
${CLICKHOUSE_CLIENT} --external_table_strict_query 1 --query "SELECT s FROM t_05260 ORDER BY s"
${CLICKHOUSE_CLIENT} --external_table_strict_query 1 --query "SELECT s FROM t_05260 WHERE u = 44" 2>&1 | grep -o 'INCORRECT_QUERY' | head -1
