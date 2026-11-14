#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema raises `max_query_size` when a stored CREATE is longer than the default limit, so the dump replays.

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

# Creates a table whose CREATE has `$2` bytes of default, dumps database d and replays the dump, both with the defaults.
replay()
{
    local name=$1 size=$2
    printf "CREATE DATABASE d; CREATE TABLE d.t (k UInt64, s String DEFAULT '%s') ENGINE = MergeTree ORDER BY k;" \
        "$(head -c "$size" /dev/zero | tr '\0' x)" > "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/$name/src" --max_query_size 1000000 -n --queries-file "$work/$name.sql" || return
    $CLICKHOUSE_LOCAL --path "$work/$name/src" --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    echo "$name: max_query_size lines: $(grep -c '^SET max_query_size = ' "$work/$name/dump.sql")"
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 100 "$work/$name/out.txt")"
}

replay short 1000
replay long 300000
