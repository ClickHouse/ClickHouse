-- Output-only correctness test for the self-join shared-scan rewrite. Only the setting under
-- test is pinned; everything else is left to the test harness's settings randomization so that
-- CI exercises arbitrary combinations. The results must be correct whether or not the rewrite
-- fires, so no plan shape is checked here.
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test

DROP TABLE IF EXISTS t_sjss_out;
CREATE TABLE t_sjss_out (x UInt64, y String, z UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_out SELECT number, toString(number), number % 7 FROM numbers(1000);
INSERT INTO t_sjss_out SELECT number, toString(number), number % 5 FROM numbers(1000, 500);

-- INNER self-join on the primary key.
SELECT count(), sum(a.x), sum(b.z) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x;

-- LEFT self-join on a non-unique column (fan-out).
SELECT count(), sum(a.x), sum(b.x) FROM t_sjss_out AS a LEFT JOIN t_sjss_out AS b ON a.z = b.z;

-- The probe side reads a strict subset of the build side's columns.
SELECT count(), max(b.y) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x;

-- Expressions between the scans and the join.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x + 1 = b.x;

-- A filter on the probe side.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x WHERE a.z = 3;

-- Aggregation on top of a fan-out join.
SELECT sum(cnt), max(cnt) FROM (SELECT a.x, count() AS cnt FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.z = b.z GROUP BY a.x);

-- Three-way self-join.
SELECT count(), sum(c.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x INNER JOIN t_sjss_out AS c ON b.x = c.x;

-- Explicit algorithm choices, including ones the rewrite is incompatible with.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'grace_hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'auto';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'hash,full_sorting_merge';

DROP TABLE t_sjss_out;
