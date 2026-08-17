#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `Remote` table whose target is a table function keeps one AST for the lifetime of the storage,
# while argument parsing rewrites that AST's argument slots in place. Concurrent readers must each
# parse their own copy, so run several readers at once and require every one to return the correct
# result. Under ThreadSanitizer a shared AST also reports a data race in
# `TableFunctionMerge::parseArguments`.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_src SELECT number FROM numbers(100);
"

# Two arguments: the database name is folded into a literal in place (the reported write).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE r_two_args (x UInt64)
        ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^t_src\$'));
"

# One argument: the single regexp argument is rewritten in place instead.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE r_one_arg (x UInt64)
        ENGINE = Remote('127.0.0.1', merge('^t_src\$'));
"

# The database name is normalized into a literal when the definition is created, and that has to
# survive: it is what a restarted server and `SHOW CREATE TABLE` read back.
$CLICKHOUSE_CLIENT -q "
    SELECT position(engine_full, format('merge(\'{}\'', currentDatabase())) > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'r_two_args'
"

# 32 readers per definition: at 8 the interleaving is too narrow for ThreadSanitizer to observe the
# unsynchronized access, at 32 it reports reliably. With the AST copied per read this costs a few
# seconds; it only becomes slow if the race is reintroduced, because generating the report stalls
# the server.
for table in r_two_args r_one_arg; do
    for _ in {1..32}; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM $table SETTINGS prefer_localhost_replica = 1" &
    done
done
wait

$CLICKHOUSE_CLIENT -q "DROP TABLE r_one_arg; DROP TABLE r_two_args; DROP TABLE t_src;"
