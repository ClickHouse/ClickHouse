-- Tests for rewrite of has([constant array], <expr>) -> <expr> IN (constant array)

SET parallel_replicas_local_plan = 1;
SET optimize_rewrite_has_to_in = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v String,
    a Array(UInt32)
)
Engine = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concat('Number', toString(number)), [number, number + 1, number + 2] from numbers(10000);

SET optimize_rewrite_has_to_in = 1;

-- basic test - 5 rows
SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id);

-- basic test - 3 rows
SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v);

-- Use an expression
SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id / 5);

-- Use an expression
SELECT COUNT(*) FROM tab WHERE has(['Numb124', 'Numb', 'Number9999'], substr(v,1,4));

-- Confirm that FUNCTION in() is seen in plan
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id)
    ) t WHERE explain like '%FUNCTION in%';

-- Confirm that FUNCTION in() is seen in plan
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v)
    ) t WHERE explain like '%FUNCTION in%';

-- Confirm that primary key index is used to skip with has -> in rewrite
SELECT COUNT(*) FROM (
    EXPLAIN indexes=1 SELECT COUNT(*) FROM tab WHERE has([82333,900000,100011], id)
    ) t WHERE explain like '%Granules: 0%';

-- Non const array, FUNCTION in() should not be seen
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(a, 5)
    ) t WHERE explain like '%FUNCTION in%';

SET optimize_rewrite_has_to_in = 0;

-- FUNCTION has() will be retained, FUNCTION in() should not be seen
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id)
    ) t WHERE explain like '%FUNCTION in%';

SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v)
    ) t WHERE explain like '%FUNCTION in%';

DROP TABLE tab;

-- Regression test for LowCardinality(Nullable(T)) needle: rewriting has() to in() would
-- change semantics on NULL values (and would also depend on transform_null_in for in()),
-- while has() returns 0 for NULL regardless. The pass must not rewrite in this case.

DROP TABLE IF EXISTS tab_lcn;

CREATE TABLE tab_lcn
(
    id UInt32,
    v LowCardinality(Nullable(String))
)
Engine = MergeTree
ORDER BY id;

INSERT INTO tab_lcn VALUES (1, 'a'), (2, 'b'), (3, NULL), (4, 'c'), (5, NULL);

SET optimize_rewrite_has_to_in = 1;

-- has() should be preserved for LowCardinality(Nullable). NULL rows return 0.
SET transform_null_in = 0;
SELECT id, has(['a', 'b'], v) FROM tab_lcn ORDER BY id;

SET transform_null_in = 1;
SELECT id, has(['a', 'b'], v) FROM tab_lcn ORDER BY id;

-- Confirm that FUNCTION in() does NOT appear in the plan for LowCardinality(Nullable) needle.
SET transform_null_in = 0;
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has(['a', 'b'], v) FROM tab_lcn
    ) t WHERE explain like '%FUNCTION in%';

SET transform_null_in = 1;
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has(['a', 'b'], v) FROM tab_lcn
    ) t WHERE explain like '%FUNCTION in%';

DROP TABLE tab_lcn;

-- Haystack with Nullable element type but no NULL values, e.g. Array(Nullable(UInt8)).
-- This currently has identical semantics to in(), but we bail out conservatively to keep
-- the pass robust against constant-folded inputs that may sneak NULLs in.
SET optimize_rewrite_has_to_in = 1;
SET transform_null_in = 0;
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has([toNullable(1), 2, 3], number) FROM numbers(5)
    ) t WHERE explain like '%FUNCTION in%';
