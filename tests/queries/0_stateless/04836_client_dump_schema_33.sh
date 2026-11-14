#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema finds the typing settings that a TTL's GROUP BY SET expressions need.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

# The table loads only with the setting on in the server's own settings, so every step runs with it.
lossy="--allow_lossy_numeric_supertype 1"
# shellcheck disable=SC2086
$CLICKHOUSE_LOCAL --path "$work/src" $lossy -n -q "CREATE DATABASE d;
    CREATE TABLE d.t (d DateTime, k UInt64, x Float64) ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 DAY GROUP BY k SET x = max(arraySum([toDecimal64(k, 2), 0.5]));"
# shellcheck disable=SC2086
$CLICKHOUSE_LOCAL --path "$work/src" $lossy --dump-schema d > "$work/dump.sql" 2> "$work/err.txt" || echo "dump failed: $(cat "$work/err.txt")"
grep -h 'typing\|supertype' "$work/err.txt"
grep '^SET allow_lossy' "$work/dump.sql"
# shellcheck disable=SC2086
$CLICKHOUSE_LOCAL --path "$work/dst" $lossy -n --queries-file "$work/dump.sql" > "$work/out.txt" 2>&1
echo "replay rc=$? $(head -c 200 "$work/out.txt")"
