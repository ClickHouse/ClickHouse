#!/usr/bin/env bash

# --dump-schema reads the query that table function `eval` generates as if it were written in its place: the tables that
# query reads are created first, with the gates it needs, and an `eval` whose query cannot be resolved statically is refused.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps them and replays the dump into a fresh target.
round_trip() {
    local name=$1 ddl=$2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

# Prints whether the dump `$1` creates `$2` before `$3`.
dumped_before() {
    local first second
    first=$(grep -n "^CREATE [A-Z ]*$2 " "$1" | head -1 | cut -d: -f1)
    second=$(grep -n "^CREATE [A-Z ]*$3 " "$1" | head -1 | cut -d: -f1)
    [ -n "$first" ] && [ -n "$second" ] && [ "$first" -lt "$second" ] && echo yes || echo no
}

# A local `Remote` table analyzes the query of its `eval` target at CREATE, so `zzz_src` and the gate of `neighbor` must come
# first. The second target folds a constant expression to a query that names its table without a database, and the
# materialized view joins the query of a literal.
round_trip eval "
    SET allow_experimental_eval_table_function = 1;
    SET allow_deprecated_error_prone_window_functions = 1;
    CREATE DATABASE d;
    USE d;
    CREATE TABLE d.base (x UInt64) ENGINE = Memory;
    CREATE VIEW d.zzz_src AS SELECT x FROM d.base;
    CREATE TABLE d.aaa_literal (x UInt64) ENGINE = Remote('127.0.0.1', eval('SELECT neighbor(x, 0) AS x FROM d.zzz_src'));
    CREATE TABLE d.aab_folded (x UInt64) ENGINE = Remote('127.0.0.1', loop(eval(concat('SELECT x FROM ', 'zzz_src'))));
    CREATE MATERIALIZED VIEW d.aac_mv ENGINE = Memory AS
        SELECT b.x AS x FROM d.base AS b INNER JOIN eval('SELECT x FROM d.zzz_src') AS e ON b.x = e.x;"
echo "eval: source before the literal query: $(dumped_before "$work/eval.sql" d.zzz_src d.aaa_literal)"
echo "eval: source before the folded query: $(dumped_before "$work/eval.sql" d.zzz_src d.aab_folded)"
echo "eval: source before the joined query: $(dumped_before "$work/eval.sql" d.zzz_src d.aac_mv)"

# A query read from a table is known only when the CREATE runs, so the tables it reads cannot be ordered before it.
rm -rf "$work/opaque"
$CLICKHOUSE_LOCAL --path "$work/opaque" -n -q "
    SET allow_experimental_eval_table_function = 1;
    CREATE DATABASE d;
    CREATE TABLE d.base (x UInt64) ENGINE = Memory;
    CREATE TABLE d.queries (q String) ENGINE = Memory;
    INSERT INTO d.queries VALUES ('SELECT x FROM d.base');
    CREATE TABLE d.aaa_opaque (x UInt64) ENGINE = Remote('127.0.0.1', eval((SELECT q FROM d.queries)));"
$CLICKHOUSE_LOCAL --path "$work/opaque" --dump-schema d > /dev/null 2> "$work/opaque.err"
echo "opaque: dump rc=$?"
grep -o 'Cannot statically resolve the query that .* generates in d.aaa_opaque' "$work/opaque.err"
