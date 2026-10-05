#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# compared by value, an ordering of a string column and an Enum constant is not pushed down as a string comparison

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
    echo "$1 $2"
    ${CLICKHOUSE_CLIENT} --compare_enum_with_string_lexicographically "$1" \
        --query "SELECT arrayStringConcat(groupArray(s), ',') FROM (SELECT s FROM t_05326 WHERE $3 ORDER BY s)"
    ${CLICKHOUSE_CLIENT} --compare_enum_with_string_lexicographically "$1" --send_logs_level=trace \
        --query "SELECT s FROM t_05326 WHERE $3 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tbl`( WHERE .*)?$'
}

check 1 's < E' "s < $E"
check 0 's < E' "s < $E"
check 0 'E > s' "$E > s"
check 0 's = E' "s = $E"
