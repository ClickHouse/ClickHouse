#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: the dump of a `Replicated` database names macros that the replaying `clickhouse-local` does not have.

# --dump-schema replays a table whose key needs a typing setting, constraint subqueries with a nested correlated
# subquery or an `eval` JOIN on a Dynamic key, a dictionary whose source table WHERE reads a view, and a reserved column name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Dumps the test database with a client on default settings, replays it into a fresh `clickhouse-local`, and drops the table.
client_round_trip() {
    local name=$1 gate=$2
    shift 2
    rm -rf "$work/dst"
    $CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/$name.sql" 2>"$work/$name.err"
    echo "$name: dump rc=$?"
    echo "$name: gate $(grep -c "^SET $gate;" "$work/$name.sql")"
    $CLICKHOUSE_LOCAL --path "$work/dst" "$@" -n --queries-file "$work/$name.sql" > "$work/$name.out" 2>&1
    echo "$name: replay rc=$?"
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
    $CLICKHOUSE_CLIENT -q "DROP TABLE $name"
}

echo '--- a constraint subquery with a nested correlated subquery ---'
$CLICKHOUSE_CLIENT -q "CREATE TABLE correlated_src (k UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --allow_experimental_correlated_subqueries=1 -q "CREATE TABLE correlated (x UInt64,
    CONSTRAINT c CHECK x < (SELECT count() FROM correlated_src AS o
        WHERE o.k = (SELECT max(i.k) FROM correlated_src AS i WHERE i.k <= o.k)) + 10) ENGINE = Memory"
client_round_trip correlated 'allow_experimental_correlated_subqueries = 1' --allow_experimental_correlated_subqueries=0

echo '--- a constraint subquery that reads an eval JOIN on a Dynamic key ---'
$CLICKHOUSE_CLIENT -q "CREATE TABLE dynamic_src (k Dynamic) ENGINE = Memory"
$CLICKHOUSE_CLIENT --allow_experimental_eval_table_function=1 --allow_dynamic_type_in_join_keys=1 -q "CREATE TABLE eval_join (x UInt64,
    CONSTRAINT c CHECK x < (SELECT * FROM eval('SELECT count() FROM ${CLICKHOUSE_DATABASE}.dynamic_src AS a
        INNER JOIN ${CLICKHOUSE_DATABASE}.dynamic_src AS b ON a.k = b.k')) + 10) ENGINE = Memory"
client_round_trip eval_join 'allow_dynamic_type_in_join_keys = 1'

# Creates the objects in a fresh source, dumps database `d`, and replays the dump into a fresh target, all with the extra settings.
round_trip() {
    local name=$1 ddl=$2
    shift 2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" --dump-schema d > "$work/$name.sql" 2> "$work/$name.err"
    echo "$name: dump rc=$?"
    $CLICKHOUSE_LOCAL --path "$work/dst" "$@" -n --queries-file "$work/$name.sql" > "$work/$name.out" 2>&1
    echo "$name: replay rc=$?"
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
}

# The server analyzes storage expressions with its own settings too, so every instance gets the setting.
echo '--- a sorting key that is a Variant without lossy numeric promotion ---'
round_trip lossy_key "
CREATE DATABASE d;
CREATE TABLE d.t (a UInt64) ENGINE = MergeTree ORDER BY if(a > 0, toDecimal64(a, 2), 0.5);" --allow_lossy_numeric_supertype=1
grep -c '^SET allow_lossy_numeric_supertype = 1;' "$work/lossy_key.sql"

echo '--- an eager dictionary comes after the view its source table WHERE reads through merge ---'
round_trip dictionary_where "
CREATE DATABASE d;
CREATE TABLE d.base (id UInt64, v UInt64) ENGINE = Memory;
CREATE VIEW d.zzz_src AS SELECT id FROM d.base;
CREATE DICTIONARY d.aaa_dict (id UInt64, v UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(DB 'd' TABLE 'base' WHERE 'id IN (SELECT id FROM merge(''d'', ''^zzz_src\$''))'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 0);"

echo '--- a column named like a virtual column ---'
round_trip reserved_name "
CREATE DATABASE d;
SET allow_reserved_column_names = 1;
CREATE TABLE d.t (_part String, x UInt64) ENGINE = MergeTree ORDER BY x;"
grep -c '^SET allow_reserved_column_names = 1;' "$work/reserved_name.sql"
