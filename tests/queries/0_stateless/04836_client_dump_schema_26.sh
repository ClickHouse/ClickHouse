#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema keeps the named-tuple setting of a stored expression whose element name is computed or read through a subcolumn.

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

# Creates `$2` in a fresh data path with the settings `$3...`, dumps database d and replays the dump into another one.
replay()
{
    local name=$1 sql=$2
    shift 2
    $CLICKHOUSE_LOCAL --path "$work/$name/src" "$@" -n -q "CREATE DATABASE d; $sql" || return
    $CLICKHOUSE_LOCAL --path "$work/$name/src" --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
}

replay computed_name "CREATE TABLE d.t (x UInt64, y UInt64 DEFAULT tupleElement(tuple(x), concat('x', ''))) ENGINE = MergeTree ORDER BY x;" \
    --enable_named_columns_in_function_tuple 1
replay subcolumn "CREATE TABLE d.t (x UInt64, y UInt64 DEFAULT getSubcolumn(tuple(x), 'x')) ENGINE = MergeTree ORDER BY x;" \
    --enable_named_columns_in_function_tuple 1
