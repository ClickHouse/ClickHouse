#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ClickHouse leaves the `NULL` members out of an `IN` set, while SQLite follows the SQL three-valued logic, so a set
# holding a `NULL` makes a negated `IN` UNKNOWN for every row it does not contain. The filter pushed to SQLite must
# select the rows the local filter selects, so the set must reach it without its `NULL` members.

BASE="${USER_FILES_PATH}/05331_sqlite_in_set_null_${CLICKHOUSE_DATABASE}"
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
CREATE TABLE tn (s TEXT, k INTEGER NOT NULL) STRICT;
INSERT INTO tn VALUES ('7', 1), ('x', 2), (NULL, 3);
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05331 (s Nullable(String), k Int64) ENGINE = SQLite('${DB_PATH}', 'tn')"

# Prints the rows, then the query sent to SQLite.
function check()
{
    echo "$1"
    ${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(groupArray(toString(k)), ',') FROM (SELECT k FROM t_05331 WHERE $1 ORDER BY k)"
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT k FROM t_05331 WHERE $1 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tn`( WHERE .*)?$'
}

check "NOT (s IN ('7', NULL))"
check "NOT (k IN (1, NULL))"
check "(s IN ('7', NULL)) = 0"
check "isNotNull(s IN ('7', NULL))"
check "NOT ((s, k) IN (('7', 1), ('x', NULL)))"
check "NOT (s IN (NULL))"
check "s IN ('7', NULL)"
check "s IN ('7', NULL, 'x')"
