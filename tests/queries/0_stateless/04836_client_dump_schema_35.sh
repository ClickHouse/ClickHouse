#!/usr/bin/env bash
# Tags: no-fasttest, long, no-flaky-check
# Tag no-flaky-check: the 60000-element CREATE is too slow for the many parallel runs of the flaky check under sanitizers.

# --dump-schema raises `max_ast_elements` for a stored CREATE bigger than its default, which the replay checks.

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

# Each ALTER stays under the default limits, but the table's stored CREATE ends up with about 60000 AST elements.
array="[$(printf 'k,%.0s' {1..19999})k]"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "CREATE DATABASE d;
    CREATE TABLE d.t (k UInt64) ENGINE = MergeTree ORDER BY k;
    ALTER TABLE d.t ADD COLUMN c0 UInt64 DEFAULT length($array);
    ALTER TABLE d.t ADD COLUMN c1 UInt64 DEFAULT length($array);
    ALTER TABLE d.t ADD COLUMN c2 UInt64 DEFAULT length($array);"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/err.txt" || echo "dump failed: $(cat "$work/err.txt")"
grep -c '^SET max_ast_elements = ' "$work/dump.sql"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" > "$work/out.txt" 2>&1
echo "replay rc=$? $(head -c 200 "$work/out.txt")"
$CLICKHOUSE_LOCAL --path "$work/dst" -q "SELECT count() FROM system.columns WHERE database = 'd' AND table = 't'"
