#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `IN` set holding an `Enum` is rendered for the external database as the local query builds it. A member
# converted to `String` next to the `Enum` must get the query's format settings there too, like `CAST` does. So must a
# `Bool` member of a set without an `Enum`.

BASE="${USER_FILES_PATH}/05331_sqlite_enum_constant_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05331"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# A STRICT table, so that the filter on these columns is pushed down.
sqlite3 "${DB_PATH}" "
CREATE TABLE tbl (s TEXT NOT NULL, b TEXT NOT NULL) STRICT;
INSERT INTO tbl VALUES ('7', 'yes'), ('7', 'true'), ('x', 'yes');
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05331 (s String, b String) ENGINE = SQLite('${DB_PATH}', 'tbl')"

E="CAST('7', 'Enum8(\\'7\\' = 3)')"

# Prints the rows, then the query sent to SQLite.
function check()
{
    echo "$1"
    ${CLICKHOUSE_CLIENT} --bool_true_representation=yes --query "SELECT arrayStringConcat(groupArray(s || ':' || b), ',') FROM (SELECT s, b FROM t_05331 WHERE $2 ORDER BY s, b)"
    ${CLICKHOUSE_CLIENT} --bool_true_representation=yes --send_logs_level=trace --query "SELECT s FROM t_05331 WHERE $2 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tbl`( WHERE .*)?$'
}

check '(s, b) IN ((E, true))' "(s, b) IN (($E, true))"
check '(s, b) IN ((E, true), (x, true))' "(s, b) IN (($E, true), ('x', true))"
check 'b IN (true)' "b IN (true)"
check '(s, b) IN ((x, true))' "(s, b) IN (('x', true))"
