#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: creates a table in the system database.

# --dump-schema does not probe a query over a table whose ALIAS column reads a file, also when that table is created
# AS a table function or lives in the system database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
system_table="system.${CLICKHOUSE_TEST_UNIQUE_NAME}"
fifo="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"
cleanup()
{
    # A reader still blocked on the FIFO gets a line and goes on.
    timeout 5 bash -c 'echo x > "$0"' "$fifo" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${system_table}"
    rm -rf "$work" "$fifo"
}
rm -rf "$work"
mkdir -p "$work"
trap cleanup EXIT
cd "$work" || exit 1

echo '--- a table created AS a table function ---'
echo hi > "$work/payload.txt"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
CREATE DATABASE d;
CREATE TABLE d.m (k UInt64, a String ALIAS file('payload.txt')) AS null();
CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT a FROM d.m;"
# A FIFO without a writer blocks whoever reads it.
rm "$work/payload.txt"
mkfifo "$work/payload.txt"
timeout 60 $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > /dev/null 2>&1
echo "table_function: dump rc=$?"

echo '--- a table in the system database ---'
rm -f "$fifo"
echo hi > "$fifo"
$CLICKHOUSE_CLIENT -n -q "
CREATE TABLE ${system_table} (k UInt64, a String ALIAS file('$(basename "$fifo")')) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT a FROM ${system_table};"
rm "$fifo"
mkfifo "$fifo"
timeout 60 $CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > /dev/null 2>&1
echo "system_table: dump rc=$?"
