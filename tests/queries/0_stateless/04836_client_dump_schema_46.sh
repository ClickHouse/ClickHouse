#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema of a data path runs no stored command: a table whose secondary index would load a dictionary,
# or whose TTL or projection holds a subquery, is refused before the command runs.

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
    grep -o -e 'would run when the table is loaded' -e 'calls the non-deterministic function' "$work/dump.err" | head -1
    echo "$name: table dumped: $(grep -c '^CREATE TABLE d.t ' "$work/dump.sql")"
    if [ -e "$work/ran" ]; then echo "$name: FAIL: a command ran"; else echo "$name: no command ran"; fi
}

# CREATE refuses a dictionary lookup in a secondary index, so such a table is one attached with its full definition.
attach="ATTACH TABLE d.t UUID '6a5d4c3b-2a19-4087-9f6e-5d4c3b2a1908'"

echo '--- a secondary index that would load a dictionary is refused ---'
dump_with_commands index_dictionary "$attach (k UInt64, INDEX i k + dictGetUInt64('d.rules', 'v', toUInt64(1)) TYPE minmax)
    ENGINE = MergeTree ORDER BY k"

echo '--- a secondary index that looks a column up in a dictionary is loaded, and the dump refuses it without a lookup ---'
dump_with_commands index_dictionary_column "$attach (k UInt64, INDEX i dictGetUInt64('d.rules', 'v', k) TYPE minmax)
    ENGINE = MergeTree ORDER BY k"

echo '--- a subquery in a TTL or a projection is refused ---'
dump_with_commands ttl_where_subquery "CREATE TABLE d.t (ts DateTime, k UInt64) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < (SELECT v FROM d.commands LIMIT 1)"
dump_with_commands ttl_where_in "CREATE TABLE d.t (ts DateTime, k UInt64) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k IN (SELECT v FROM d.commands)"
dump_with_commands projection_subquery "CREATE TABLE d.t (k UInt64, PROJECTION p (SELECT k + (SELECT v FROM d.commands LIMIT 1) ORDER BY k))
    ENGINE = MergeTree ORDER BY k"

echo '--- a TTL with a condition and a projection without a subquery are dumped ---'
dump_with_commands plain "CREATE TABLE d.t (ts DateTime, k UInt64, INDEX i k + 1 TYPE minmax, PROJECTION p (SELECT k + 1 ORDER BY k))
    ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k < 7"
