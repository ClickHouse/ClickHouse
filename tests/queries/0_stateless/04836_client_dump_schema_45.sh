#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema orders an eager dictionary after what its local `CLICKHOUSE` source reads, however the local host is spelled
# and also when only a setting of the source query reads it. A source on another host or port gives no such order.

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
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
}

eager='SETTINGS(dictionary_lazy_load = 0)'
through_merge="QUERY 'SELECT id, v FROM merge(''d'', ''^zzz_src\$'')'"

echo '--- the local host in another letter case ---'
round_trip uppercase_host "HOST 'LOCALHOST' $through_merge" "$eager"

echo '--- a table that only a setting of the source query reads ---'
round_trip query_setting "QUERY 'SELECT toUInt64(0) AS id, toUInt64(0) AS v SETTINGS filter = ''id IN (SELECT id FROM d.zzz_src)'''" "$eager"

# These dictionaries are lazy: nothing connects to the other server.
echo '--- another host is not a local dependency ---'
round_trip remote_host "HOST '192.0.2.1' $through_merge" ''

echo '--- the local host on a port that is not the server port is not a local dependency ---'
round_trip foreign_port "HOST 'localhost' PORT 1 $through_merge" ''
