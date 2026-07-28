SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
-- Randomized in CI, and the plans below differ between its two values.
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], [3, 4], [5, 6]);

-- Only n.a is used — n.b and n.c should not be read.
SELECT n.a FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- Verify nested() is pruned to only a.
EXPLAIN QUERY TREE SELECT n.a FROM t_nested ARRAY JOIN n ORDER BY n.a;
EXPLAIN header = 1 SELECT n.a FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- Both n.a and n.b used — n.c should not be read.
SELECT n.a, n.b FROM t_nested ARRAY JOIN n ORDER BY n.a;

EXPLAIN QUERY TREE SELECT n.a, n.b FROM t_nested ARRAY JOIN n ORDER BY n.a;
EXPLAIN header = 1 SELECT n.a, n.b FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- Direct reference to n — all subcolumns needed.
SELECT n FROM t_nested ARRAY JOIN n ORDER BY n.a;

EXPLAIN QUERY TREE SELECT n FROM t_nested ARRAY JOIN n ORDER BY n.a;
EXPLAIN header = 1 SELECT n FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- n used only in WHERE — should still be pruned to only n.a.
SELECT 1 FROM t_nested ARRAY JOIN n WHERE n.a > 0;

EXPLAIN QUERY TREE SELECT 1 FROM t_nested ARRAY JOIN n WHERE n.a > 0;
EXPLAIN header = 1 SELECT 1 FROM t_nested ARRAY JOIN n WHERE n.a > 0;

-- Numeric tupleElement index — should prune the same as string access.
SELECT tupleElement(n, 1) FROM t_nested ARRAY JOIN n ORDER BY n.a;
EXPLAIN header = 1 SELECT tupleElement(n, 1) FROM t_nested ARRAY JOIN n ORDER BY n.a;

DROP TABLE t_nested;

-- General case: ARRAY JOIN with two independent arrays, only one used.
-- The unused array a is replaced by an offsets-only carrier rather than removed: a and b must have
-- equal per-row sizes, which is validated at execution, so a must still reach that check, but only
-- its lengths are read (issue #111747).
DROP TABLE IF EXISTS t_two_arrays;
CREATE TABLE t_two_arrays (a Array(Int64), b Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_two_arrays VALUES ([1, 2], [3, 4]);

SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

-- Verify: a is still an ARRAY JOIN operand, as a carrier of its own lengths.
EXPLAIN QUERY TREE SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

-- Verify with EXPLAIN header=1 that only a's sizes, not its values, are read from storage.
EXPLAIN header = 1 SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

-- Same for count(), where BOTH operands are unused: neither array's values are read.
SELECT count() FROM t_two_arrays ARRAY JOIN a, b;
EXPLAIN header = 1 SELECT count() FROM t_two_arrays ARRAY JOIN a, b;

-- Robust form of the two assertions above, so they survive plan-format churn: the sizes are read and
-- the full arrays are not. The last row is the live control for the Array(Int64) pattern -- b IS read
-- in full when it is used, so a pattern that never matches anything would fail there.
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_two_arrays ARRAY JOIN a, b)
    WHERE explain ILIKE '%a.size0 UInt64%';
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_two_arrays ARRAY JOIN a, b)
    WHERE explain ILIKE '%b.size0 UInt64%';
SELECT count() FROM (EXPLAIN header = 1 SELECT count() FROM t_two_arrays ARRAY JOIN a, b)
    WHERE explain ILIKE '%Array(Int64)%';
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b)
    WHERE explain ILIKE '%a.size0 UInt64%';
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b)
    WHERE explain ILIKE '%b Array(Int64)%';

DROP TABLE t_two_arrays;
