#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema does not probe a query over a table whose ALIAS column checks `IN` a table that reads a file to resolve.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
cleanup()
{
    # A reader still blocked on the FIFO gets a line and goes on.
    timeout 5 bash -c 'echo x > "$0"' "$work/payload.csv" 2>/dev/null
    rm -rf "$work"
}
rm -rf "$work"
mkdir -p "$work"
trap cleanup EXIT
cd "$work" || exit 1

echo 1 > "$work/payload.csv"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
CREATE DATABASE d;
CREATE DATABASE e;
CREATE TABLE e.f AS file('payload.csv');
CREATE TABLE d.t (k UInt64, b UInt8 ALIAS k IN e.f) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT b FROM d.t;"
# A FIFO without a writer blocks whoever reads it.
rm "$work/payload.csv"
mkfifo "$work/payload.csv"
timeout 60 $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > /dev/null 2>&1
echo "alias_in_table: dump rc=$?"
