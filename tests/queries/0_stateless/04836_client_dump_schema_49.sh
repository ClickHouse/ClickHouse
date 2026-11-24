#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema sets the typing settings that a materialized view needs for the ALIAS columns its SELECT reads, which CREATE
# expands, as probed whatever the dump session's values, and keeps an earlier table's typing settings from leaking into a
# CREATE that picks none.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps `d` with the given dump session settings, prints the typing settings before
# each CREATE, and replays the dump into a fresh target.
round_trip() {
    local name=$1 ddl=$2
    shift 2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" --dump-schema d > "$work/$name.sql" 2> "$work/$name.err"
    echo "$name: dump rc=$?"
    grep -h 'typing' "$work/$name.err"
    grep -o -E '^SET allow_lossy_numeric_supertype = [01]|^CREATE (TABLE|MATERIALIZED VIEW) d\.[a-z_]+' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/$name.sql" > "$work/$name.out" 2>&1
    echo "$name: replay rc=$?"
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
}

# `d.b_lossy` needs numeric promotion, and the ALIAS column of `d.a_src` refuses it.
objects="
CREATE DATABASE d;
SET allow_lossy_numeric_supertype = 1;
CREATE TABLE d.b_lossy (a UInt64, x Float64 DEFAULT arraySum([toDecimal64(a, 2), 0.5])) ENGINE = Memory;
SET allow_lossy_numeric_supertype = 0;
CREATE TABLE d.a_src (a UInt64, v String ALIAS toString(variantType(if(a > 0, toDecimal64(a, 2), 0.5)))) ENGINE = Memory;
CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT v FROM d.a_src;"

echo '--- the default session ---'
round_trip default "$objects"

echo '--- a session with numeric promotion ---'
round_trip lossy "$objects" --allow_lossy_numeric_supertype=1

# The dump does not see what `merge` reads, so this table picks no typing setting, but its CREATE runs the subquery.
echo '--- a table that picks no typing setting ---'
round_trip merge "$objects
CREATE TABLE d.c_merge (a UInt64, CONSTRAINT c CHECK (SELECT count() FROM merge('d', '^a_src\$') WHERE v != '') >= 0)
    ENGINE = Memory;"
