#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A string column is compared with an `Enum` constant by the Enum's name, a numeric column by its value. The
# filter pushed to the external database must select the rows the local filter selects, so the constant must
# reach it in that form, also inside a tuple, an `IN` list, a `Nullable`, a `Variant` or a `Dynamic`.

BASE="${USER_FILES_PATH}/05321_sqlite_enum_constant_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05321"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# A STRICT table, so that the filter on these columns is pushed down.
sqlite3 "${DB_PATH}" "
CREATE TABLE tbl (s TEXT NOT NULL, n INTEGER NOT NULL, f REAL NOT NULL) STRICT;
INSERT INTO tbl VALUES ('7', 3, 3.0), ('3', 3, 7.0), ('x', 42, 42.0);
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05321 (s String, n Int64, f Float64) ENGINE = SQLite('${DB_PATH}', 'tbl')"

E="CAST('7', 'Enum8(\\'7\\' = 3)')"
E2="CAST('3', 'Enum8(\\'3\\' = 4)')"
VE="CAST(CAST('7', 'Enum8(\\'7\\' = 3, \\'3\\' = 4)') AS Variant(Enum8('7' = 3, '3' = 4), Array(UInt8)))"
VE2="CAST(CAST('3', 'Enum8(\\'7\\' = 3, \\'3\\' = 4)') AS Variant(Enum8('7' = 3, '3' = 4), Array(UInt8)))"
VS="Variant(String, Array(UInt8))"
VT="CAST(CAST(($E, 4), 'Tuple(Enum8(\\'7\\' = 3), UInt8)') AS Variant(Tuple(Enum8('7' = 3), UInt8), String))"

# Prints the rows, then the query sent to SQLite.
function check()
{
    echo "$1"
    ${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(groupArray(s), ',') FROM (SELECT s FROM t_05321 WHERE $2 ORDER BY s)"
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT s FROM t_05321 WHERE $2 FORMAT Null" 2>&1 \
        | grep -oE 'Query: SELECT .* FROM `tbl`( WHERE .*)?$'
}

check 's = E' "s = $E"
check 'E = s' "$E = s"
check 's != E' "s != $E"
check 's < E' "s < $E"
check 's IN (E)' "s IN ($E)"
check 's IN (E, E2)' "s IN ($E, $E2)"
check 's NOT IN (E)' "s NOT IN ($E)"
check 's IN (E, x)' "s IN ($E, 'x')"
check '(s, n) IN ((E, 3))' "(s, n) IN (($E, 3))"
check '(s, n) IN ((E, 3), (E2, 3))' "(s, n) IN (($E, 3), ($E2, 3))"
check '(s, n) < (E, 4)' "(s, n) < ($E, 4)"
check '(s, n) < (E, 2.5)' "(s, n) < ($E, 2.5)"
check 's = toNullable(E)' "s = toNullable($E)"
check 's = E OR n = 42' "s = $E OR n = 42"
check 's = Dynamic(E)' "s = CAST($E AS Dynamic)"
check 's = Dynamic(max_types = 0)(E)' "s = CAST($E AS Dynamic(max_types = 0))"
check 's IN (Dynamic(E), Dynamic(E2))' "s IN (CAST($E AS Dynamic), CAST($E2 AS Dynamic))"
check 's = Variant(E)' "s = $VE"
check 'n = Variant(E)' "n = $VE"
check 's IN (Variant(E), Variant(E2))' "s IN ($VE, $VE2)"
check 'n IN (Variant(E), Variant(E2))' "n IN ($VE, $VE2)"
check 's IN (Variant(7), Variant(x))' "s IN (CAST('7' AS $VS), CAST('x' AS $VS))"
check '(s, f) < (E, toDecimal64(4, 1))' "(s, f) < ($E, toDecimal64(4, 1))"
check 'n = E' "n = $E"
check 'n IN (E)' "n IN ($E)"
check 'f = E' "f = $E"
check '(s, n) < Nullable((E, 4))' "(s, n) < toNullable(($E, 4))"
check '(s, n) < Dynamic((E, 4))' "(s, n) < CAST(($E, 4) AS Dynamic)"
check '(s, n) < Variant((E, 4))' "(s, n) < $VT"
check '(s, n) IN (Nullable((E, 3)))' "(s, n) IN (toNullable(($E, 3)))"
check 'tuple(s) IN (tuple(E))' "tuple(s) IN (tuple($E))"
check 'tuple(s) IN (tuple(E), tuple(E2))' "tuple(s) IN (tuple($E), tuple($E2))"

echo '(s, n) < (E, x)'
${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05321 WHERE (s, n) < ($E, 'x')" 2>&1 | grep -o 'TYPE_MISMATCH' | head -1
