#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema finds the typing settings that an index needs for a type its validator accepts, whatever the index type.

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

# The index validator runs with the server's own settings, so every step runs with the setting on.
lossy="--allow_lossy_numeric_supertype 1"

# Creates `$2`, dumps database d, replays the dump and compares the replayed table with the source one.
replay()
{
    local name=$1 sql=$2
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/src" $lossy -n -q "CREATE DATABASE d; $sql" || return
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/src" $lossy --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    grep -h 'typing\|supertype' "$work/$name/err.txt"
    grep '^SET allow_lossy' "$work/$name/dump.sql"
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" $lossy -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
    for side in src dst; do
        # shellcheck disable=SC2086
        $CLICKHOUSE_LOCAL --path "$work/$name/$side" $lossy -q "SHOW CREATE TABLE d.t" > "$work/$name/$side.txt" 2>&1
    done
    cmp -s "$work/$name/src.txt" "$work/$name/dst.txt" && echo "$name: same table" || echo "$name: tables differ"
}

# With the setting off the expression is a `Variant`, which analyzes but which these indexes refuse.
replay bloom_filter "CREATE TABLE d.t (a UInt64, INDEX i if(a > 0, toDecimal64(a, 2), 0.5) TYPE bloom_filter) ENGINE = MergeTree ORDER BY a;"
replay bloom_filter_array "CREATE TABLE d.t (a UInt64, INDEX i [toDecimal64(a, 2), 0.5] TYPE bloom_filter) ENGINE = MergeTree ORDER BY a;"
replay vector_similarity "CREATE TABLE d.t (a UInt64, INDEX i [toDecimal64(a, 2), 0.5] TYPE vector_similarity('hnsw', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY a;"
# A set index takes a `Variant`, so the table needs no setting.
replay set "CREATE TABLE d.t (a UInt64, INDEX i if(a > 0, toDecimal64(a, 2), 0.5) TYPE set(10)) ENGINE = MergeTree ORDER BY a;"
