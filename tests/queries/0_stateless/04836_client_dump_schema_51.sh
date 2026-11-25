#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema of a data path dumps a table whose TTL or projection holds a subquery of literals only, and refuses one whose
# subquery reads a table or names a column or an alias, or that names a table on the right of IN, before a command runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Loads a source with a table and a dictionary that both run a command, and the given table, and tells whether the dump ran a command.
dump_with_commands() {
    local name=$1
    rm -rf "$work/src" "$work/ran"
    mkdir -p "$work/src/user_scripts"
    printf '#!/bin/sh\necho ran >> %s/ran\necho 7\n' "$work" > "$work/src/user_scripts/probe.sh"
    chmod +x "$work/src/user_scripts/probe.sh"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.commands (v UInt64) ENGINE = Executable('probe.sh', 'TSV');
    CREATE DICTIONARY d.rules (k UInt64, v UInt64) PRIMARY KEY k
        SOURCE(EXECUTABLE(COMMAND 'echo ran >> $work/ran; echo 1,7' FORMAT 'CSV')) LAYOUT(FLAT()) LIFETIME(0);
    $2;"
    rm -f "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/dump.err"
    echo "$name: dump failed: $([ $? -ne 0 ] && echo 1 || echo 0)"
    grep -o 'would run when the table is loaded' "$work/dump.err" | head -1
    echo "$name: table dumped: $(grep -c '^CREATE TABLE d.t ' "$work/dump.sql")"
    if [ -e "$work/ran" ]; then echo "$name: FAIL: a command ran"; else echo "$name: no command ran"; fi
}

table="CREATE TABLE d.t (ts DateTime, k UInt64"

echo '--- a subquery of literals only in a TTL or a projection is dumped ---'
dump_with_commands projection_in_constant "$table, PROJECTION p (SELECT k WHERE k IN (SELECT 1) ORDER BY k)) ENGINE = MergeTree ORDER BY k"
dump_with_commands ttl_where_constant "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k < (SELECT 7)"

echo '--- a subquery that reads a table, or names a column or an alias, is refused ---'
dump_with_commands ttl_where_nested "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k IN (SELECT (SELECT v FROM d.commands LIMIT 1))"
dump_with_commands ttl_where_in_table "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k IN (SELECT 7 WHERE 7 IN d.commands)"
dump_with_commands ttl_where_with_dictionary "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < (WITH toUInt64(1) AS c SELECT dictGetUInt64('d.rules', 'v', c))"

echo '--- a dictionary lookup on constants inside a subquery of literals is refused ---'
dump_with_commands ttl_where_dictionary "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < (SELECT dictGetUInt64('d.rules', 'v', toUInt64(1)))"

echo '--- a table named on the right of IN in a TTL or a projection, or in an ALIAS column a projection reads, is refused ---'
dump_with_commands ttl_where_in_named_table "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k NOT IN d.commands"
dump_with_commands projection_in_named_table "$table, PROJECTION p (SELECT k WHERE k IN d.commands ORDER BY k)) ENGINE = MergeTree ORDER BY k"
dump_with_commands projection_alias_in_named_table "$table, a UInt8 ALIAS k IN d.commands, PROJECTION p (SELECT k, a ORDER BY k))
    ENGINE = MergeTree ORDER BY k"
