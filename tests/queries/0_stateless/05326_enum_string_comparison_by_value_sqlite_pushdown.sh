#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# a string column ordered against an Enum constant compares by value, so the condition stays local

BASE="${USER_FILES_PATH}/05326_sqlite_enum_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05326"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" "
CREATE TABLE tbl (s TEXT NOT NULL) STRICT;
INSERT INTO tbl VALUES ('z'), ('a'), ('m');
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05326 (s String) ENGINE = SQLite('${DB_PATH}', 'tbl')"

E="CAST('m', 'Enum8(\\'z\\' = 1, \\'a\\' = 2, \\'m\\' = 3)')"

# prints the rows, then the query sent to SQLite
function check()
{
    echo "$1"
    ${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(groupArray(s), ',') FROM (SELECT s FROM t_05326 WHERE $2 ORDER BY s)"
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT s FROM t_05326 WHERE $2 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tbl`( WHERE .*)?$'
}

check 's < E' "s < $E"
check 'E > s' "$E > s"
check 's = E' "s = $E"
check 'toString(E) > s' "toString($E) > s"
