#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema orders an eager dictionary after what its local `CLICKHOUSE` source reads: with a quoted `SECURE` flag,
# with a repeated `QUERY`, of which the source runs the first, and when only a setting of the dictionary reads it.
# A source on a host that is not shown to be this server gives no such order, only a warning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates `d.zzz_src` and the dictionary `d.aaa_dict` in a fresh source, dumps `d`, prints the order of the two in the dump,
# and replays the dump into a fresh target.
round_trip() {
    local name=$1 source=$2 settings=$3
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.zzz_src (id UInt64, v UInt64) ENGINE = Memory;
    CREATE DICTIONARY d.aaa_dict (id UInt64, v UInt64) PRIMARY KEY id
        SOURCE(CLICKHOUSE($source)) LAYOUT(FLAT()) LIFETIME(0) $settings;"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/$name.sql" 2> "$work/$name.err"
    echo "$name: dump rc=$?"
    echo "$name: order $(grep -o -E '^CREATE (TABLE|DICTIONARY) d\.[a-z_]+' "$work/$name.sql" | sed 's/.* d\.//' | tr '\n' ' ' | sed 's/ $//')"
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/$name.sql" > "$work/$name.out" 2>&1
    echo "$name: replay rc=$?"
    grep -o 'Warning: .* through [^,]*' "$work/$name.err"
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
}

eager='SETTINGS(dictionary_lazy_load = 0)'
through_merge="QUERY 'SELECT id, v FROM merge(''d'', ''^zzz_src\$'')'"
constant="QUERY 'SELECT toUInt64(0) AS id, toUInt64(0) AS v'"

echo '--- a quoted flag ---'
round_trip quoted_secure "SECURE 'false' $through_merge" "$eager"

echo '--- the first of two queries is the one that runs ---'
round_trip repeated_query "$through_merge $constant" "$eager"
round_trip repeated_query_unread "$constant $through_merge" "$eager"

echo '--- a table that only a setting of the dictionary reads ---'
round_trip dictionary_setting "$constant" "SETTINGS(dictionary_lazy_load = 0, filter = 'id IN (SELECT id FROM d.zzz_src)')"

# This dictionary is lazy: nothing connects to the other server.
echo '--- a host that is not shown to be this server only warns ---'
round_trip unknown_host "HOST '192.0.2.1' $through_merge" ''
