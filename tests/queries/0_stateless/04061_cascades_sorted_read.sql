-- Tests for `SortedReadImplementation`, `LocalReadImplementation`, and
-- `StreamingAggregationStrategy` in the Cascades optimizer.
--
-- Key behaviors verified:
-- 1. ORDER BY on PK: `SortedRead` eliminates the explicit Sorting step via
--    `ReadType: InOrder`.
-- 2. No ORDER BY: `LocalReadImplementation` provides unsorted fallback.
-- 3. ORDER BY on non-PK column: Sorting step is present, `ReadType: Default`.
-- 4. GROUP BY on PK: streaming aggregation on `SortedRead` input — no hash table.
-- 5. GROUP BY on non-PK: hash aggregation (streaming can't help).

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_sorted;
DROP TABLE IF EXISTS t_no_key;

CREATE TABLE t_sorted (a UInt64, b String, c Float64) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t_no_key (x UInt64, y String) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_sorted SELECT number, toString(number), number * 0.5 FROM numbers(100);
INSERT INTO t_no_key SELECT number, toString(number) FROM numbers(100);

-- 1. ORDER BY on PK: SortedRead eliminates Sort step, ReadType=InOrder.
SELECT '-- 1. ORDER BY a (PK): SortedRead, no Sort';
EXPLAIN actions = 1, sorting = 1 SELECT a FROM t_sorted ORDER BY a LIMIT 10;

-- 2. ORDER BY DESC on PK: SortedRead in reverse, ReadType=InOrder.
SELECT '-- 2. ORDER BY a DESC: SortedRead, no Sort';
EXPLAIN actions = 1, sorting = 1 SELECT a FROM t_sorted ORDER BY a DESC LIMIT 10;

-- 3. No ORDER BY: no Sorting step, no SortedRead, ReadType=Default.
SELECT '-- 3. No ORDER BY: ReadType=Default';
EXPLAIN actions = 1 SELECT a FROM t_sorted;

-- 4. ORDER BY on non-PK column: Sorting step present, ReadType=Default.
SELECT '-- 4. ORDER BY non-PK: Sorting present';
EXPLAIN actions = 1, sorting = 1 SELECT a FROM t_sorted ORDER BY c;

-- 5. Table without sorting key + ORDER BY: Sorting present, ReadType=Default.
SELECT '-- 5. No sorting key + ORDER BY';
EXPLAIN actions = 1, sorting = 1 SELECT x FROM t_no_key ORDER BY x;

-- 6. Correctness: ORDER BY ASC.
SELECT '-- 6. Correctness ASC';
SELECT a FROM t_sorted ORDER BY a LIMIT 5;

-- 7. Correctness: ORDER BY DESC.
SELECT '-- 7. Correctness DESC';
SELECT a FROM t_sorted ORDER BY a DESC LIMIT 5;

-- 8. GROUP BY on PK: streaming aggregation with SortedRead, ReadType=InOrder.
--    The streaming variant calls `applyOrder` on the cloned `AggregatingStep`,
--    which uses `AggregatingInOrderTransform` — linear scan, no hash table.
SELECT '-- 8. GROUP BY a (PK): streaming agg + SortedRead';
EXPLAIN actions = 1 SELECT a, count() FROM t_sorted GROUP BY a;

-- 9. GROUP BY on non-PK: hash aggregation, ReadType=Default.
SELECT '-- 9. GROUP BY b (non-PK): hash agg';
EXPLAIN actions = 1 SELECT b, count() FROM t_sorted GROUP BY b;

-- 10. Correctness: GROUP BY on PK.
SELECT '-- 10. Correctness GROUP BY PK';
SELECT a, count() FROM t_sorted GROUP BY a ORDER BY a LIMIT 5
SETTINGS make_distributed_plan = 0;

-- 11. Correctness: GROUP BY on non-PK.
SELECT '-- 11. Correctness GROUP BY non-PK';
SELECT b, count() FROM t_sorted GROUP BY b ORDER BY b LIMIT 5
SETTINGS make_distributed_plan = 0;

DROP TABLE t_sorted;
DROP TABLE t_no_key;
