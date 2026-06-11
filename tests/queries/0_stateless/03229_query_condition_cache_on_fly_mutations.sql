-- Tags: no-parallel
-- no-parallel: drops the (instance-wide) query condition cache

-- Regression test for the query condition cache being poisoned by on-fly mutations.
-- A query with apply_mutations_on_fly = 1 applies pending UPDATE/DELETE mutations as a
-- filter ahead of PREWHERE, so a granule can become fully non-matching only because of
-- the mutation. Recording such granules under the PREWHERE predicate's hash poisoned the
-- cache: a later query with apply_mutations_on_fly = 0 (same predicate) wrongly skipped
-- them and returned too few rows. index_granularity = 1 makes the effect deterministic
-- because each granule holds a single row.

SET use_query_condition_cache = 1;

-- Case 1: on-fly DELETE
DROP TABLE IF EXISTS t_qcc_on_fly_delete;

CREATE TABLE t_qcc_on_fly_delete (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_qcc_on_fly_delete SELECT number, number FROM numbers(100);

SET mutations_sync = 0;
SYSTEM STOP MERGES t_qcc_on_fly_delete;
ALTER TABLE t_qcc_on_fly_delete DELETE WHERE id % 3 = 0;

SYSTEM DROP QUERY CONDITION CACHE;

-- apply_mutations_on_fly = 1 first (writes the would-be poisoned entry): 20 rows match id % 5 = 0,
-- of which 7 (ids 0,15,30,45,60,75,90) are deleted on fly -> 13.
SELECT count() FROM t_qcc_on_fly_delete PREWHERE id % 5 = 0 SETTINGS apply_mutations_on_fly = 1;
-- apply_mutations_on_fly = 0 must ignore the pending DELETE -> all 20 rows.
SELECT count() FROM t_qcc_on_fly_delete PREWHERE id % 5 = 0 SETTINGS apply_mutations_on_fly = 0;

DROP TABLE t_qcc_on_fly_delete;

-- Case 2: on-fly UPDATE that moves the PREWHERE column out of the matching range
DROP TABLE IF EXISTS t_qcc_on_fly_update;

CREATE TABLE t_qcc_on_fly_update (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_qcc_on_fly_update SELECT number, number FROM numbers(100);

SET mutations_sync = 0;
SYSTEM STOP MERGES t_qcc_on_fly_update;
ALTER TABLE t_qcc_on_fly_update UPDATE v = 0 WHERE id >= 50;

SYSTEM DROP QUERY CONDITION CACHE;

-- apply_mutations_on_fly = 1 first: the update sets v = 0 for ids >= 50, so no row has v >= 50 -> 0.
SELECT count() FROM t_qcc_on_fly_update PREWHERE v >= 50 SETTINGS apply_mutations_on_fly = 1;
-- apply_mutations_on_fly = 0 must see the original values -> ids 50..99 -> 50.
SELECT count() FROM t_qcc_on_fly_update PREWHERE v >= 50 SETTINGS apply_mutations_on_fly = 0;

DROP TABLE t_qcc_on_fly_update;

-- Case 3: same on-fly UPDATE but the predicate stays in WHERE (move-to-prewhere disabled), so the
-- cache is written by the downstream FilterTransform rather than the PREWHERE attribution path.
DROP TABLE IF EXISTS t_qcc_on_fly_where;

CREATE TABLE t_qcc_on_fly_where (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_qcc_on_fly_where SELECT number, number FROM numbers(100);

SET mutations_sync = 0;
SYSTEM STOP MERGES t_qcc_on_fly_where;
ALTER TABLE t_qcc_on_fly_where UPDATE v = 0 WHERE id >= 50;

SYSTEM DROP QUERY CONDITION CACHE;

-- apply_mutations_on_fly = 1 first: v = 0 for ids >= 50, so no row has v >= 50 -> 0.
SELECT count() FROM t_qcc_on_fly_where WHERE v >= 50 SETTINGS apply_mutations_on_fly = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
-- apply_mutations_on_fly = 0 must see the original values -> ids 50..99 -> 50.
SELECT count() FROM t_qcc_on_fly_where WHERE v >= 50 SETTINGS apply_mutations_on_fly = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

DROP TABLE t_qcc_on_fly_where;
