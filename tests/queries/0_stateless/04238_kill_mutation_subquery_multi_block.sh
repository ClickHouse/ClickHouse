#!/usr/bin/env bash
# Regression for https://github.com/ClickHouse/ClickHouse/issues/104857
# (STID 4569-3618).
#
# `KILL MUTATION` / `KILL QUERY` used to throw the LOGICAL_ERROR
# `Expected one block from input stream` when the internal `SELECT` over a
# `system.*` table produced more than one block. AST-fuzzer triggered this
# via per-row subqueries in `WHERE` combined with a small `max_block_size`.
# The fix collects all produced blocks and concatenates them, instead of
# asserting at most one.
#
# The reproducer runs in `clickhouse local` (rather than against the test
# server) because the underlying LOGICAL_ERROR is a `chassert` that aborts
# the process in debug and sanitizer builds. With `clickhouse-client` the
# abort lands inside the long-running test server, which causes the test
# runner's hung-check to terminate the runner before any test result is
# recorded, so the `Bugfix validation` framework cannot invert the result
# to `OK`. Running the reproducer in `clickhouse local` contains the abort
# to a single short-lived subprocess: the test runner observes a non-zero
# exit and an empty stdout, both of which are diff'd against the
# `.reference` file and reported as a normal `FAIL`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --multiquery -q "
DROP TABLE IF EXISTS t_kill_mut_multi_block;
CREATE TABLE t_kill_mut_multi_block (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_kill_mut_multi_block SELECT number, number FROM numbers(5);

-- Queue pending mutations so \`system.mutations\` has rows for our database.
SYSTEM STOP MERGES t_kill_mut_multi_block;
ALTER TABLE t_kill_mut_multi_block UPDATE c1 = c1 + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_kill_mut_multi_block UPDATE c1 = c1 + 2 WHERE 1 SETTINGS mutations_sync = 0;

-- Per-row subquery in \`WHERE\` combined with a small \`max_block_size\` forces
-- the internal \`SELECT database, table, mutation_id, command FROM system.mutations\`
-- to emit multiple blocks. \`TEST\` prevents real cancellation, so this is
-- exercising only the SELECT-collection path that used to assert.
KILL MUTATION
    WHERE database = currentDatabase()
      AND ((SELECT 1 WHERE database = command) OR command LIKE '%UPDATE%')
    TEST
    SETTINGS max_block_size = 1
    FORMAT Null;

SELECT 'ok';
"
