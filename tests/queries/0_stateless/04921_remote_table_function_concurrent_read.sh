#!/usr/bin/env bash
# Tags: shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `Remote` table whose target is a table function keeps one AST for the lifetime of the storage,
# while argument parsing rewrites that AST's argument slots in place. Concurrent operations must each
# parse their own copy, so run several at once and require every one to produce the correct result.
# Under ThreadSanitizer a shared AST also reports a data race in
# `TableFunctionMerge::parseArguments`. Reads and parallel `INSERT SELECT` reach that AST through
# different code, so both are exercised below.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS r_one_arg;
    DROP TABLE IF EXISTS r_two_args;
    DROP TABLE IF EXISTS r_dst;
    DROP TABLE IF EXISTS t_dst;
    DROP TABLE IF EXISTS t_src;
"

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

# A destination for the parallel `INSERT SELECT` phase. A table-function target cannot be written to,
# and more than one shard would need a sharding key, so this is a single shard over a local table.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE r_dst (x UInt64) ENGINE = Remote('127.0.0.1', currentDatabase(), 't_dst');
"

# The database name is normalized into a literal when the definition is created, and that has to
# survive: it is what a restarted server and `SHOW CREATE TABLE` read back.
$CLICKHOUSE_CLIENT -q "
    SELECT position(engine_full, format('merge(\'{}\'', currentDatabase())) > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'r_two_args'
"

# 32 concurrent readers per definition: at 8 the interleaving is too narrow for ThreadSanitizer to
# observe the unsynchronized access, at 32 it reports reliably. One process holds all 32 connections,
# because an instrumented client costs a few hundred megabytes and the test's process group runs
# under a 10 GiB memory limit on sanitizer builds.
# Each exit status is checked, because a benchmark whose every query failed still leaves the
# assertions below unchanged and would otherwise report nothing.
reader_pids=()
for table in r_two_args r_one_arg; do
    $CLICKHOUSE_BENCHMARK -c 32 -i 32 -d 0 \
        <<< "SELECT count() FROM $table SETTINGS prefer_localhost_replica = 1" >/dev/null 2>&1 &
    reader_pids+=("$!")
done
for pid in "${reader_pids[@]}"; do
    wait "$pid" || echo "concurrent reads failed with status $?"
done

# Read on the local shard, as the concurrent phase above does. The single-argument form resolves the
# table against the current database, so a shard that received the query would look in its own.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM r_two_args SETTINGS prefer_localhost_replica = 1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM r_one_arg SETTINGS prefer_localhost_replica = 1"

# Parallel `INSERT SELECT` rewrites the statement only on its optimized route and falls back silently
# otherwise, so the route is asserted rather than assumed: only that route ships a rewritten statement
# carrying the table function to the shard, where it is logged as a child query.
OPT_QUERY_ID="04921_${CLICKHOUSE_DATABASE}_optimized"
$CLICKHOUSE_CLIENT --query_id "$OPT_QUERY_ID" -q "
    INSERT INTO r_dst SELECT * FROM r_two_args
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0
"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# A query the shard ran carries `current_database = 'default'`, not the initiator's database, so the
# child row is scoped to this test through its initiator instead.
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM system.query_log
    WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
      AND query LIKE 'INSERT INTO%merge(%'
      AND initial_query_id IN (
          SELECT query_id FROM system.query_log
          WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND is_initial_query = 1 AND query_id = '$OPT_QUERY_ID'
      )
    SETTINGS enable_parallel_replicas = 0
"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_dst"

# The same statement concurrently, which is what races the shared AST on that path. The query is on one
# line because standard input here is read as one query per line.
$CLICKHOUSE_BENCHMARK -c 16 -i 16 -d 0 \
    <<< "INSERT INTO r_dst SELECT * FROM r_two_args SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0" \
    >/dev/null 2>&1

$CLICKHOUSE_CLIENT -q "SELECT count() = 1700 FROM t_dst"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS r_one_arg;
    DROP TABLE IF EXISTS r_two_args;
    DROP TABLE IF EXISTS r_dst;
    DROP TABLE IF EXISTS t_dst;
    DROP TABLE IF EXISTS t_src;
"
