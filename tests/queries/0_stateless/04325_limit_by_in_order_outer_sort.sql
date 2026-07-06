-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 4;

-- { echo }

-- An in-order LIMIT BY reads several already-sorted streams and combines them with `resize(1)` in
-- arbitrary order, so its output is not globally sorted by the LIMIT BY columns. An outer ORDER BY on
-- those columns must therefore be a full sort, not a FinishSorting that assumes them already ordered.
DROP TABLE IF EXISTS test_outer_order_by;
CREATE TABLE test_outer_order_by (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES test_outer_order_by;
INSERT INTO test_outer_order_by SELECT number, number         FROM numbers(10000);
INSERT INTO test_outer_order_by SELECT number, number + 10000 FROM numbers(10000);

-- Each key `a` has two rows, so `LIMIT 2 BY a` keeps both and the twelve smallest by `a` are 0,0,1,1,2,2,3,3,4,4,5,5.
SELECT a FROM (SELECT a FROM test_outer_order_by LIMIT 2 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a LIMIT 12;

-- The twelve largest by `a` are 9999,9999,...,9994,9994.
SELECT a FROM (SELECT a FROM test_outer_order_by LIMIT 2 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a DESC LIMIT 12;

-- The twenty smallest rows are each of 0..9 twice, checked as a sorted multiset so the assertion does not
-- depend on the order `groupArray` collects them in.
SELECT arraySort(groupArray(a)) FROM (SELECT a FROM (SELECT a FROM test_outer_order_by LIMIT 2 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a LIMIT 20);

-- The outer ORDER BY stays a full Sorting and carries no FinishSorting prefix from the LIMIT BY.
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM (SELECT a FROM test_outer_order_by LIMIT 2 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a) WHERE explain ILIKE '%Prefix sort description%';

-- The LIMIT BY itself still reads in order, using the streaming transform.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a FROM test_outer_order_by LIMIT 2 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBySortedStreamTransform%';

DROP TABLE test_outer_order_by;

-- When the input is a single globally-sorted stream (here from the inner ORDER BY a), the LIMIT BY keeps
-- that order, so an outer ORDER BY on a superset of those columns is correctly optimized into a
-- FinishSorting over the preserved prefix.
DROP TABLE IF EXISTS test_outer_finish_sort;
CREATE TABLE test_outer_finish_sort (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES test_outer_finish_sort;
INSERT INTO test_outer_finish_sort SELECT number % 5, number FROM numbers(20);

-- `LIMIT 100 BY a` keeps every row; the result is fully sorted by (a, v).
SELECT a, v FROM (SELECT a, v FROM test_outer_finish_sort ORDER BY a LIMIT 100 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a, v;

-- The outer ORDER BY a, v is a FinishSorting over the preserved `a` prefix. The inner ORDER BY a is a plain
-- full sort (the table is not ordered by `a`), so the single prefix sort description in the plan is the outer one.
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, v FROM (SELECT a, v FROM test_outer_finish_sort ORDER BY a LIMIT 100 BY a SETTINGS optimize_limit_by_in_order = 1) ORDER BY a, v) WHERE explain ILIKE '%Prefix sort description%';

DROP TABLE test_outer_finish_sort;
