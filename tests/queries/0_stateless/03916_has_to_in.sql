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

-- Regression tests for type-compatibility guard. has() on a constant array compares raw
-- Field values, while in() converts the right-hand side to the left-hand side type. For
-- types whose Field representation has different semantic meaning (Date vs DateTime,
-- Enum vs String, IPv4 vs UInt32), the two diverge — the rewrite must not happen.

-- Date vs DateTime: has() returns 0 (raw Field comparison of 20464 days vs ~1.7e9 seconds),
-- in() would return 1 (DateTime is converted to Date). Verify has() result is preserved
-- and that FUNCTION in() does not appear in the plan.
SELECT has([toDateTime('2026-01-10 12:34:56')], toDate('2026-01-10'));
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has([toDateTime('2026-01-10 12:34:56')], materialize(toDate('2026-01-10')))
    ) t WHERE explain like '%FUNCTION in%';

-- Enum vs String: has() returns 0 (Field-level Enum integer vs String), in() would return 1.
SELECT has(['a', 'b'], CAST('a' AS Enum8('a' = 1, 'b' = 2)));
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has(['a', 'b'], materialize(CAST('a' AS Enum8('a' = 1, 'b' = 2))))
    ) t WHERE explain like '%FUNCTION in%';

-- IPv4 vs UInt32: in() throws on type mismatch, has() works. Rewrite would change behavior.
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has([toIPv4('1.2.3.4')], materialize(toUInt32(16909060)))
    ) t WHERE explain like '%FUNCTION in%';

-- Same-type case (Date vs Date): the rewrite is allowed.
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has([toDate('2026-01-10')], materialize(toDate('2026-01-10')))
    ) t WHERE explain like '%FUNCTION in%';

-- Native-number cross-width (Array(UInt16) vs UInt32): the rewrite is allowed because both
-- sides reduce to a single numeric Field type and accurateEquals handles mixed widths.
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT has([toUInt16(1), toUInt16(2)], materialize(toUInt32(1)))
    ) t WHERE explain like '%FUNCTION in%';

-- Regression test for LowCardinality(non-nullable) needle: has() returns UInt8 but
-- `lc_col IN (...)` returns LowCardinality(UInt8). Rewriting would leave parent nodes
-- (e.g. NOT, AND, equals) with a stale UInt8 expectation and trip a logical error.
-- Found by fuzzer: `SELECT count() FROM t WHERE NOT has(['abc', 'x9999'], p)` where
-- p is LowCardinality(String).
DROP TABLE IF EXISTS tab_lc;

CREATE TABLE tab_lc
(
    id UInt32,
    p LowCardinality(String)
)
Engine = MergeTree
ORDER BY id;

INSERT INTO tab_lc VALUES (1, 'abc'), (2, 'def'), (3, 'x9999'), (4, 'ghi');

SET optimize_rewrite_has_to_in = 1;

SELECT COUNT(*) FROM tab_lc WHERE NOT has(['abc', 'x9999'], p);
SELECT COUNT(*) FROM tab_lc WHERE has(['abc', 'x9999'], p);

-- The rewrite must not happen for LowCardinality(non-nullable) needles either.
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab_lc WHERE NOT has(['abc', 'x9999'], p)
    ) t WHERE explain like '%FUNCTION in%';

DROP TABLE tab_lc;
