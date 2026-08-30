-- `EXPLAIN ANALYZE` over a `Merge` table with several children, a bare-column `PREWHERE` and a
-- separate `WHERE` used to fail with `Logical error: 'Required output position 1 is out of range for
-- pass-through inputs'`: every child shared one `PrewhereInfo`, and pruning the first child's plan
-- flipped the shared `remove_prewhere_column`, invalidating the sibling headers already derived from
-- it. `EXPLAIN ANALYZE` is required because it optimizes the whole plan only after every child
-- exists; on the plain `SELECT` path each child is pruned as it is built.
--
-- `query_plan_remove_unused_columns` is randomized by the test runner and both settings below are
-- required to reach the path, so they are pinned per query. `viewExplain` keeps the assertion stable:
-- raw `EXPLAIN ANALYZE` output carries timings.

DROP TABLE IF EXISTS t04814_a;
DROP TABLE IF EXISTS t04814_b;
DROP TABLE IF EXISTS t04814_c;
DROP TABLE IF EXISTS t04814_m;

-- Fixture 1: identical child structures, so both children share one cached query info.
CREATE TABLE t04814_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t04814_b (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04814_a SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t04814_b SELECT number + 1000, number FROM numbers(1000);
CREATE TABLE t04814_m ENGINE = Merge(currentDatabase(), '^t04814_[ab]$');

SELECT 'identical structures, aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT count() FROM t04814_m PREWHERE k WHERE v != 0))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

SELECT 'identical structures, non-aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT v FROM t04814_m PREWHERE k WHERE v != 0 ORDER BY v LIMIT 1))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

-- The plain `SELECT` path is correct today; these controls prove the fix only changes ownership of
-- the filter DAG, not results.
SELECT 'identical structures, results';
SELECT count() FROM t04814_m PREWHERE k WHERE v != 0;
SELECT count() FROM t04814_m WHERE k AND v != 0;
SELECT sum(v) FROM t04814_m PREWHERE k WHERE v != 0;

DROP TABLE t04814_m;
DROP TABLE t04814_b;

-- Fixture 2: differing child structures, so the children do not share a cached query info. Both
-- shapes must be covered: they reach the fix site through different branches.
CREATE TABLE t04814_b (k UInt64, v UInt64, extra String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04814_b SELECT number + 1000, number, 'x' FROM numbers(1000);
CREATE TABLE t04814_m ENGINE = Merge(currentDatabase(), '^t04814_[ab]$');

SELECT 'differing structures, aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT count() FROM t04814_m PREWHERE k WHERE v != 0))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

SELECT 'differing structures, non-aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT v FROM t04814_m PREWHERE k WHERE v != 0 ORDER BY v LIMIT 1))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

SELECT 'differing structures, results';
SELECT count() FROM t04814_m PREWHERE k WHERE v != 0;
SELECT count() FROM t04814_m WHERE k AND v != 0;
SELECT sum(v) FROM t04814_m PREWHERE k WHERE v != 0;

DROP TABLE t04814_m;
DROP TABLE t04814_b;

-- Fixture 3: three identical children. The first child misses the query-info cache and the other
-- two hit it, so two children reach the fix site through the cache-hit branch. Two children are not
-- enough for that: one takes each branch, so a fix applied to only one branch still hands out two
-- distinct objects and looks correct.
CREATE TABLE t04814_b (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t04814_c (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04814_b SELECT number + 1000, number FROM numbers(1000);
INSERT INTO t04814_c SELECT number + 2000, number + 5 FROM numbers(1000);
CREATE TABLE t04814_m ENGINE = Merge(currentDatabase(), '^t04814_[abc]$');

SELECT 'three identical children, aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT count() FROM t04814_m PREWHERE k WHERE v != 0))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

SELECT 'three identical children, non-aggregate';
SELECT count() > 0 FROM viewExplain('EXPLAIN ANALYZE', '', (SELECT v FROM t04814_m PREWHERE k WHERE v != 0 ORDER BY v LIMIT 1))
SETTINGS query_plan_remove_unused_columns = 1, query_plan_filter_push_down = 1;

SELECT 'three identical children, results';
SELECT count() FROM t04814_m PREWHERE k WHERE v != 0;
SELECT count() FROM t04814_m WHERE k AND v != 0;
SELECT sum(v) FROM t04814_m PREWHERE k WHERE v != 0;

DROP TABLE t04814_m;
DROP TABLE t04814_c;
DROP TABLE t04814_b;
DROP TABLE t04814_a;
