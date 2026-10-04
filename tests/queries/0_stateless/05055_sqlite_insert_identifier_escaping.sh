#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Fast tests don't build external libraries (SQLite)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ANCHOR="${USER_FILES_PATH}/05055_anchor_${CLICKHOUSE_DATABASE}.db"
# Outside the user_files confinement.
OUTSIDE="${CLICKHOUSE_TMP}/05055_outside_${CLICKHOUSE_DATABASE}.db"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_name_inj"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_col_inj"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_quote_id"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_backslash_id"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_nul_name"
    rm -f "${ANCHOR}" "${OUTSIDE}"
}
trap cleanup EXIT
cleanup

# Odd names: the old backslash escaping produced them, so the injected script's first statement succeeds and the rest runs.
sqlite3 "${ANCHOR}" 'CREATE TABLE "target\"(id INTEGER)'
sqlite3 "${ANCHOR}" 'CREATE TABLE col_anchor("c\" INTEGER)'
sqlite3 "${ANCHOR}" 'CREATE TABLE "ta""ble" ("c""1" INTEGER)'
sqlite3 "${ANCHOR}" 'CREATE TABLE "a\b" (x INTEGER)'
chmod ugo+rw "${ANCHOR}"

# A database outside user_files, holding a canary the user must never be able to reach.
sqlite3 "${OUTSIDE}" "CREATE TABLE secrets(s TEXT); INSERT INTO secrets VALUES ('CANARY')"
chmod ugo+rw "${OUTSIDE}"

# One stable token per failure, so the reference embeds no path and no server version.
classify()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --query="$1" 2>&1 \
        | grep -oF -e 'no such table' -e 'no column named' -e 'unrecognized token' \
                   -e 'syntax error' -e 'PATH_ACCESS_DENIED' \
        | sed -n 1p)
    echo "${out:-NO_ERROR}"
}

# Read out of band: the ClickHouse read path cannot address these names, so an in-band readback would be a false negative.
injected_state()
{
    sqlite3 "${ANCHOR}" "SELECT 'objects created in the anchor: ' || coalesce((SELECT group_concat(name) FROM (SELECT name FROM sqlite_master WHERE name LIKE '%\_marker' ESCAPE '\' ORDER BY name)), 'none')"
    sqlite3 "${ANCHOR}" "SELECT 'canary read from outside user_files: ' || coalesce((SELECT group_concat(s) FROM stolen_marker), 'none')" 2>/dev/null \
        || echo 'canary read from outside user_files: none'
    sqlite3 "${OUTSIDE}" "SELECT 'objects written outside user_files: ' || coalesce((SELECT group_concat(name) FROM (SELECT name FROM sqlite_master WHERE name LIKE '%\_marker' ESCAPE '\' ORDER BY name)), 'none')"
}

echo '--- A1 control: the boundary the injection bypasses is enforced when the path is named directly'
classify "CREATE TABLE t_ctl (s String) ENGINE = SQLite('${OUTSIDE}', 'secrets')"

echo '--- A2 injection through the remote table name'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_name_inj (id UInt32) ENGINE = SQLite('${ANCHOR}', \$\$target\" (id) VALUES (999); ATTACH DATABASE '${OUTSIDE}' AS v; CREATE TABLE stolen_marker AS SELECT s FROM v.secrets; CREATE TABLE v.written_marker(z); DETACH v; --\$\$)"
classify "INSERT INTO t_name_inj VALUES (0)"
injected_state

echo '--- A3 injection through a column name'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_col_inj (\`c') VALUES (1); CREATE TABLE col_marker(z); --\` UInt32) ENGINE = SQLite('${ANCHOR}', 'col_anchor')"
classify "INSERT INTO t_col_inj VALUES (0)"
injected_state

echo '--- A4 control: a table and column name that legitimately contain a double quote round-trip'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_quote_id (\`c\"1\` UInt32) ENGINE = SQLite('${ANCHOR}', 'ta\"ble')"
classify "INSERT INTO t_quote_id VALUES (11)"
sqlite3 "${ANCHOR}" 'SELECT "c""1" FROM "ta""ble"'

echo '--- A5 control: a table name that legitimately contains a backslash round-trips'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_backslash_id (x UInt32) ENGINE = SQLite('${ANCHOR}', 'a\\\\b')"
classify "INSERT INTO t_backslash_id VALUES (22)"
sqlite3 "${ANCHOR}" 'SELECT x FROM "a\b"'

echo '--- A6 a NUL in the remote table name fails loudly, it is never silently truncated'
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_nul_name (id UInt32) ENGINE = SQLite('${ANCHOR}', 'a\0b')"
classify "INSERT INTO t_nul_name VALUES (0)"
