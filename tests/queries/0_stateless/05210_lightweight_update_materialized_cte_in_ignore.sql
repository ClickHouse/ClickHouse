-- A lightweight UPDATE / DELETE whose predicate feeds an `IN` subquery that defines a
-- reused MATERIALIZED CTE into `ignore`. The `in` result is then consumed by `ignore`
-- instead of being a condition of the predicate, so the mutation plan gates nothing,
-- while the set's source plan still reads the CTE's `StorageMemory`. More than one
-- reading stream is needed for that source to be scheduled, hence several parts.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';
-- Pin the second carrier: the mutation must read the table with more than one stream.
SET max_threads = 3;
-- The multi-stream pin below is read back from `system.query_log`.
SET log_queries = 1;

DROP TABLE IF EXISTS t_lwu_cte_in_ignore;

CREATE TABLE t_lwu_cte_in_ignore (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_lwu_cte_in_ignore;

INSERT INTO t_lwu_cte_in_ignore VALUES (0, 0);
INSERT INTO t_lwu_cte_in_ignore VALUES (1, 1);
INSERT INTO t_lwu_cte_in_ignore VALUES (2, 2);

-- Carrier pin 1: the reused CTE is really materialized and not inlined, so the `IN`
-- subquery keeps a `StorageMemory` reader that the gate has to hold back. A planner
-- change that inlines it would make the rest of this test vacuous.
SELECT countIf(explain LIKE '%MaterializingCTE%') > 0
FROM (EXPLAIN WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);

-- Carrier pin 2: the table is read from three separate parts, so the read is multi-stream.
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_cte_in_ignore' AND active;

-- `ignore` makes the predicate constant zero, so nothing is updated; the point is that
-- the CTE readers of the run-time set must not run before the CTE is materialized.
UPDATE t_lwu_cte_in_ignore SET v = 100 WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b));
SELECT * FROM t_lwu_cte_in_ignore ORDER BY id;

-- Carrier pin 3: that mutation really did read every part, i.e. its own plan was
-- multi-stream - with a single reading stream no reader of the set's source plan is
-- scheduled and the bug this test guards is invisible.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SelectedParts'] > 1
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND query_kind = 'Update' AND query LIKE '%SET v = 100%'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The same with a second, ordinary `IN` conjunct whose set is needed.
UPDATE t_lwu_cte_in_ignore SET v = 200 WHERE (v IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b)) AND ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b));
SELECT * FROM t_lwu_cte_in_ignore ORDER BY id;

-- A lightweight DELETE goes through the same mutation plan.
DELETE FROM t_lwu_cte_in_ignore WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b));
SELECT * FROM t_lwu_cte_in_ignore ORDER BY id;

-- Positive control: the same CTE reached through a predicate that does use the `in`
-- result keeps working and sees the materialized rows.
UPDATE t_lwu_cte_in_ignore SET v = 300 WHERE id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_cte_in_ignore ORDER BY id;

-- Positive control: the `SELECT` counterpart of the first mutation.
SELECT count() FROM t_lwu_cte_in_ignore WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b));

DROP TABLE t_lwu_cte_in_ignore;
