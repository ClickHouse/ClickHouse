SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

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
DROP TABLE IF EXISTS t_two_arrays;
CREATE TABLE t_two_arrays (a Array(Int64), b Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_two_arrays VALUES ([1, 2], [3, 4]);

SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

-- Verify: column a should be pruned from ARRAY JOIN, only b remains.
EXPLAIN QUERY TREE SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

-- Verify with EXPLAIN header=1 that only b is read from storage.
EXPLAIN header = 1 SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

DROP TABLE t_two_arrays;
