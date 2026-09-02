-- Tests the query-plan pass that short-circuits the read of a JOIN input side that can never
-- contribute a row when the ON condition folds to a constant false. Issue #110225: the
-- non-contributing side must not be read. The JoinStep is kept in place, so join validation
-- still runs and results are unchanged.

SET enable_analyzer = 1;
SET query_plan_short_circuit_constant_false_join = 1;

-- Plan checks: assert the optimization fires. A non-contributing side becomes a `ReadNothing`
-- source while the `Join` step is retained. Use the robust `SELECT ... FROM (EXPLAIN ...)` form.

SELECT 'INNER JOIN ON false -> both inputs become empty sources';
SELECT count() = 2 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    INNER JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'LEFT JOIN ON false -> right (null) side is an empty source';
SELECT count() = 1 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'RIGHT JOIN ON false -> left (null) side is an empty source';
SELECT count() = 1 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    RIGHT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'FULL JOIN ON false -> both sides preserved, NOT short-circuited';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    FULL JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'Constant-false from predicate folding (a.t = ''A'' AND a.t = ''B'')';
SELECT count() = 1 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x, toString(number) AS t FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND a.t = 'A' AND a.t = 'B'
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'A true ON condition is NOT short-circuited';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'LEFT SEMI JOIN ON false -> both inputs become empty sources';
SELECT count() = 2 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    LEFT SEMI JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'RIGHT SEMI JOIN ON false -> both inputs become empty sources';
SELECT count() = 2 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    RIGHT SEMI JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'The setting = 0 disables the optimization (no ReadNothing)';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    INNER JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
    SETTINGS query_plan_short_circuit_constant_false_join = 0
) WHERE explain ILIKE '%ReadNothing%';

-- Read-rows check: the non-contributing side must not be scanned. The runtime join can only
-- cancel the probe side after the build side is filled, so a big build (right) side of a LEFT
-- join was fully read before this optimization; here it is read as zero rows.

CREATE TABLE l (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(10);
CREATE TABLE r (y UInt64) ENGINE = MergeTree ORDER BY y AS SELECT number FROM numbers(1000000);

SELECT count() FROM l a LEFT JOIN r b ON a.x = b.y AND 1 = 2
    SETTINGS log_comment = '04512_left_read_rows';
SELECT count() FROM l a LEFT ANTI JOIN r b ON a.x = b.y AND 1 = 2
    SETTINGS log_comment = '04512_left_anti_read_rows';

SYSTEM FLUSH LOGS query_log;

SELECT 'read_rows < 1000 (a full right scan would be > 1000000)', read_rows < 1000
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04512_left_read_rows', '04512_left_anti_read_rows')
    AND type = 'QueryFinish'
ORDER BY log_comment;

-- Result checks: the short-circuit must not change results (each constant-false join returns
-- exactly the rows below). Pin join_use_nulls for the exact-output rows (CI randomizes it, and it
-- changes the null-side value from 0 to NULL); the two rows that exercise join_use_nulls = 1 set
-- it locally.
SET join_use_nulls = 0;

SELECT 'Results are unchanged';
SELECT 'INNER', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    INNER JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'LEFT', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    LEFT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'RIGHT', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    RIGHT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'FULL', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    FULL JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;

SELECT 'LEFT join_use_nulls', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    LEFT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2
    ORDER BY a.x, b.y SETTINGS join_use_nulls = 1;
SELECT 'FULL join_use_nulls', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    FULL JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2
    ORDER BY a.x, b.y SETTINGS join_use_nulls = 1;

SELECT 'LEFT SEMI', a.x FROM (SELECT number AS x FROM numbers(5)) a
    LEFT SEMI JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x;
SELECT 'LEFT ANTI', a.x FROM (SELECT number AS x FROM numbers(5)) a
    LEFT ANTI JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x;

-- Validation is preserved: because the JoinStep is kept (not replaced by an empty source), an
-- invalid constant-false join over a Join-engine table still throws instead of being silently
-- short-circuited to empty (rework of the pre-validation approach; guards 02498 behavior).
CREATE TABLE mt (key1 UInt64, key2 UInt64, key3 UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE sj (key2 UInt64, key1 UInt64, key3 UInt64, attr UInt64) ENGINE = Join(ALL, INNER, key3, key2, key1);
SELECT 'StorageJoin ON 0 still validates';
SELECT * FROM mt ALL INNER JOIN sj ON 0; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN,INVALID_JOIN_ON_EXPRESSION }

-- The pass is currently skipped for distributed plans, so a constant-false join still executes
-- normally here.
CREATE TABLE dist (k UInt64) ENGINE = MergeTree ORDER BY k AS SELECT number FROM numbers(10);
SELECT 'Distributed plan is not broken by the short-circuit';
-- make_distributed_plan rejects parallel replicas and a non-zero max_rows_to_group_by; pin both
-- off (the functional-test profile sets max_rows_to_group_by = 10G by default).
SELECT count() FROM dist a INNER JOIN dist b ON a.k = b.k AND 1 = 2
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
             enable_parallel_replicas = 0,
             max_rows_to_group_by = 0;
