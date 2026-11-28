#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema of a data path runs no command through a table-function structure: a table created `AS` a table function whose
# structure string, literal or computed, has a column default that would load a dictionary is refused, and a materialized view
# reading such a table function is not probed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Loads a source with a dictionary whose source runs a command, and the given objects, and tells whether the dump ran it.
dump_with_commands() {
    local name=$1
    rm -rf "$work/src" "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.src (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE DICTIONARY d.rules (k UInt64, v UInt64) PRIMARY KEY k
        SOURCE(EXECUTABLE(COMMAND 'echo ran >> $work/ran; echo 1,7' FORMAT 'CSV')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE FUNCTION d_rule AS () -> dictGetUInt64('d.rules', 'v', toUInt64(1));
    CREATE NAMED COLLECTION d_source AS url = 'http://127.0.0.1:1/unused', format = 'TSV';
    $2;"
    rm -f "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/dump.err"
    echo "$name: dump failed: $([ $? -ne 0 ] && echo 1 || echo 0)"
    grep -o 'in the structure of [a-zA-Z]* would run when the table is loaded' "$work/dump.err" | head -1
    echo "$name: dumped: $(grep -c -e '^CREATE TABLE d.t ' -e '^CREATE MATERIALIZED VIEW d.mv ' "$work/dump.sql")"
    if [ -e "$work/ran" ]; then echo "$name: FAIL: a command ran"; else echo "$name: no command ran"; fi
}

source="'http://127.0.0.1:1/unused', 'TSV'"
rule="dictGetUInt64(''d.rules'', ''v'', toUInt64(1))"
mv="CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT s.x AS x FROM d.src AS s INNER JOIN"

echo '--- a table created AS a table function whose structure has a default that loads a dictionary is refused ---'
dump_with_commands url_default "CREATE TABLE d.t AS url($source, 'x UInt64 DEFAULT $rule')"
dump_with_commands url_function "CREATE TABLE d.t AS url($source, 'x UInt64 DEFAULT d_rule()')"
dump_with_commands url_columns "CREATE TABLE d.t (x UInt64) AS url($source, 'x UInt64 DEFAULT $rule')"
dump_with_commands url_named_structure "CREATE TABLE d.t AS url(d_source, structure = 'x UInt64 DEFAULT $rule')"
dump_with_commands values_structure "CREATE TABLE d.t AS values('x UInt64 DEFAULT $rule', 1)"
dump_with_commands generate_random_computed "CREATE TABLE d.t AS generateRandom(concat('x UInt64 DEFAULT ', '$rule'))"
dump_with_commands url_computed "CREATE TABLE d.t AS url($source, concat('x UInt64 DEFAULT ', '$rule'))"
dump_with_commands generate_random_alias "CREATE TABLE d.t AS generateRandom('k UInt64, a UInt64 ALIAS toUInt64(1),
    x UInt64 DEFAULT dictGetUInt64(''d.rules'', ''v'', a)')"

echo '--- a materialized view reading a table function whose structure has such a default is not probed ---'
dump_with_commands mv_url "$mv url($source, 'x UInt64 DEFAULT $rule') AS u ON s.x = u.x"
dump_with_commands mv_values "$mv values('x UInt64 DEFAULT $rule', 1) AS u ON s.x = u.x"
dump_with_commands mv_null_computed "$mv null(concat('x UInt64 DEFAULT ', '$rule')) AS u ON s.x = u.x"

echo '--- a default that looks a column up, or loads nothing, is not run, and the table is dumped ---'
dump_with_commands url_column_lookup "CREATE TABLE d.t AS url($source, 'k UInt64, x UInt64 DEFAULT dictGetUInt64(''d.rules'', ''v'', k)')"
dump_with_commands generate_random_alias_column "CREATE TABLE d.t AS generateRandom('k UInt64, a UInt64 ALIAS k + 1,
    x UInt64 DEFAULT dictGetUInt64(''d.rules'', ''v'', a)')"
dump_with_commands url_plain "CREATE TABLE d.t AS url($source, 'k UInt64, x UInt64 DEFAULT k + 1')"
dump_with_commands mv_url_plain "$mv url($source, 'k UInt64, x UInt64 DEFAULT k + 1') AS u ON s.x = u.x"
