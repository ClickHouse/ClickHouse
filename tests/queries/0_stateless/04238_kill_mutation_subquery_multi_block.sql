-- Regression for https://github.com/ClickHouse/ClickHouse/issues/104857
-- (STID 4569-3618).
--
-- `KILL MUTATION` / `KILL QUERY` used to throw the LOGICAL_ERROR
-- `Expected one block from input stream` when the internal `SELECT` over a
-- `system.*` table produced more than one block. AST-fuzzer triggered this
-- via per-row subqueries in `WHERE` combined with a small `max_block_size`.
-- The fix concatenates all produced blocks instead of asserting at most one.

DROP TABLE IF EXISTS t_kill_mut_multi_block;
CREATE TABLE t_kill_mut_multi_block (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_kill_mut_multi_block SELECT number, number FROM numbers(5);

-- Queue pending mutations so `system.mutations` has rows for our database.
SYSTEM STOP MERGES t_kill_mut_multi_block;
ALTER TABLE t_kill_mut_multi_block UPDATE c1 = c1 + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_kill_mut_multi_block UPDATE c1 = c1 + 2 WHERE 1 SETTINGS mutations_sync = 0;

-- Per-row subquery in `WHERE` combined with a small `max_block_size` forces
-- the internal `SELECT database, table, mutation_id, command FROM system.mutations`
-- to emit multiple blocks. `TEST` prevents real cancellation, so this is
-- exercising only the SELECT-collection path that used to assert.
KILL MUTATION
    WHERE database = currentDatabase()
      AND ((SELECT 1 WHERE database = command) OR command LIKE '%UPDATE%')
    TEST
    SETTINGS max_block_size = 1
    FORMAT Null;

SELECT 'ok';

SYSTEM START MERGES t_kill_mut_multi_block;
DROP TABLE t_kill_mut_multi_block;
