#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema finds the typing settings that `ifNotFinite` and the engine arguments need, and proves them on every
# expression of the CREATE, so the dump replays on a server with the default settings.

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

# Creates `$2` with `allow_lossy_numeric_supertype` on, dumps database d and replays the dump, both with the defaults.
replay()
{
    local name=$1 sql=$2
    $CLICKHOUSE_LOCAL --path "$work/$name/src" -n -q "CREATE DATABASE d; SET allow_lossy_numeric_supertype = 1; $sql" || return
    $CLICKHOUSE_LOCAL --path "$work/$name/src" --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    grep -h 'typing\|supertype\|function_tuple' "$work/$name/err.txt"
    grep '^SET allow_lossy\|^SET enable_named' "$work/$name/dump.sql"
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
}

decimal_sum="arraySum([toDecimal64(k, 2), 0.5])"

# `ifNotFinite` types its result like `if`.
replay if_not_finite "CREATE TABLE d.t (k UInt64, x Float64 DEFAULT greatest(ifNotFinite(toFloat64(k), toDecimal64(1, 2)), 0))
    ENGINE = MergeTree ORDER BY k;"
# The `tuple` of the columns to sum is probed too, so it does not keep the default from being probed.
replay engine_arguments "CREATE TABLE d.t (k UInt64, x Float64, y Float64 DEFAULT $decimal_sum)
    ENGINE = SummingMergeTree((x, y)) ORDER BY k;"
