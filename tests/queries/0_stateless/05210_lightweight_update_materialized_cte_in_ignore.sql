-- A lightweight UPDATE / DELETE whose predicate feeds an `IN` subquery that defines a
-- reused MATERIALIZED CTE into `ignore`. The `in` result is then consumed by `ignore`
-- instead of being a condition of the predicate, so the mutation plan gates nothing,
-- while the set's source plan still reads the CTE's `StorageMemory`. More than one
-- reading stream is needed for that source to be scheduled, hence several parts.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DROP TABLE IF EXISTS t_lwu_cte_in_ignore;

CREATE TABLE t_lwu_cte_in_ignore (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_lwu_cte_in_ignore;

INSERT INTO t_lwu_cte_in_ignore VALUES (0, 0);
INSERT INTO t_lwu_cte_in_ignore VALUES (1, 1);
INSERT INTO t_lwu_cte_in_ignore VALUES (2, 2);

-- `ignore` makes the predicate constant zero, so nothing is updated; the point is that
-- the CTE readers of the run-time set must not run before the CTE is materialized.
UPDATE t_lwu_cte_in_ignore SET v = 100 WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b));
SELECT * FROM t_lwu_cte_in_ignore ORDER BY id;

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
