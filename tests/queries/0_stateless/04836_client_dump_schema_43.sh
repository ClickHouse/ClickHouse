#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema of a data path runs no stored command: a subquery of a constraint is not run, and a table whose arguments
# hold a subquery, or whose TTL `GROUP BY ... SET` would load a dictionary, is refused.

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
    CREATE TABLE d.t $2;"
    rm -f "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/dump.err"
    echo "$name: dump rc=$?"
    grep -o 'Code: 48' "$work/dump.err" | head -1
    echo "$name: table dumped: $(grep -c '^CREATE TABLE d.t ' "$work/dump.sql")"
    if [ -e "$work/ran" ]; then echo "$name: FAIL: a command ran"; else echo "$name: no command ran"; fi
}

echo '--- a constraint subquery is not run, and the table is dumped ---'
dump_with_commands constraint_table "(x UInt64, CONSTRAINT c CHECK x < (SELECT max(v) FROM d.commands)) ENGINE = Memory"
grep -c 'CONSTRAINT c CHECK x < (SELECT max(v) FROM d.commands)' "$work/dump.sql"
dump_with_commands constraint_dictionary "(x UInt64, CONSTRAINT c CHECK x < (SELECT dictGetUInt64('d.rules', 'v', toUInt64(1))))
    ENGINE = MergeTree ORDER BY x"

echo '--- a subquery in the arguments of a table function is refused ---'
dump_with_commands argument_subquery "(x UInt64)
    AS url((SELECT concat('http://127.0.0.1:1/', toString(max(v))) FROM d.commands), 'TSV', 'x UInt64')"

echo '--- a subquery inside the table function that a remote table reads through is not run, and the table is dumped ---'
dump_with_commands nested_query "(x UInt64) AS remote('127.0.0.1', view(SELECT (SELECT max(v) FROM d.commands) AS x))"
dump_with_commands nested_argument "(x UInt64) ENGINE = Remote('127.0.0.1',
    url((SELECT concat('http://127.0.0.1:1/', toString(max(v))) FROM d.commands), 'TSV', 'x UInt64'))"

echo '--- a TTL GROUP BY SET that would load a dictionary is refused ---'
dump_with_commands ttl_set "(ts DateTime, k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k
    TTL ts GROUP BY k SET x = max(x) + dictGetUInt64('d.rules', 'v', toUInt64(1))"

echo '--- a TTL GROUP BY SET that looks a column up in a dictionary is dumped ---'
dump_with_commands ttl_set_column "(ts DateTime, k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k
    TTL ts GROUP BY k SET x = max(dictGetUInt64('d.rules', 'v', x))"
