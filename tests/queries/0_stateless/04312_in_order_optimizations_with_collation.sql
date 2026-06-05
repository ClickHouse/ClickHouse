-- In-order optimizations (DISTINCT in order, LIMIT BY in order) must not be applied across a sort
-- that uses a collator: it orders rows by collation key, not by value, so equal values are not
-- adjacent (e.g. 'a' and 'A' under a case-insensitive collation), which these optimizations rely on.

-- { echo }

-- The in-order LIMIT BY transform is produced only by the new planner, so pin the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ENGINE = MergeTree ORDER BY tuple() AS SELECT if(number % 2 = 0, 'a', 'A') FROM numbers(100000);

-- DISTINCT in order + collation: distinct values of s are {'a', 'A'} -> exactly 2, independent of order.
SELECT count()
FROM (SELECT DISTINCT s FROM (SELECT if(number % 2 = 0, 'a', 'A') AS s FROM numbers(1000) ORDER BY s COLLATE 'en-u-ks-level2'))
SETTINGS optimize_distinct_in_order = 1, max_block_size = 7;

-- DISTINCT in order + collation: exact values, new analyzer.
SELECT arraySort(groupArray(s))
FROM (SELECT DISTINCT s FROM (SELECT arrayJoin(['a', 'A', 'a', 'A', 'á', 'Á', 'b']) AS s ORDER BY s COLLATE 'en-u-ks-level2'))
SETTINGS optimize_distinct_in_order = 1;

-- DISTINCT in order + collation: exact values, old analyzer.
SELECT arraySort(groupArray(s))
FROM (SELECT DISTINCT s FROM (SELECT arrayJoin(['a', 'A', 'a', 'A', 'á', 'Á', 'b']) AS s ORDER BY s COLLATE 'en-u-ks-level2'))
SETTINGS optimize_distinct_in_order = 1, enable_analyzer = 0;

-- DISTINCT in order is NOT used when the sort key is collated.
SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT s FROM (SELECT arrayJoin(['a', 'A']) AS s ORDER BY s COLLATE 'en-u-ks-level2') SETTINGS optimize_distinct_in_order = 1)
WHERE explain ILIKE '%DistinctSorted%';

-- DISTINCT in order is still used without a collator.
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT s FROM (SELECT arrayJoin(['a', 'A']) AS s ORDER BY s) SETTINGS optimize_distinct_in_order = 1)
WHERE explain ILIKE '%DistinctSorted%';

-- LIMIT BY in order + collation: one row per distinct value of s; distinct values {'a', 'A'} -> 2.
SELECT count()
FROM (SELECT if(number % 2 = 0, 'a', 'A') AS s FROM numbers(1000) ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s)
SETTINGS max_block_size = 7;

-- LIMIT BY in order + collation: exact values, new analyzer.
SELECT arraySort(groupArray(s))
FROM (SELECT arrayJoin(['a', 'A', 'a', 'A', 'á', 'Á', 'b']) AS s ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s);

-- LIMIT BY in order + collation: exact values, old analyzer.
SELECT arraySort(groupArray(s))
FROM (SELECT arrayJoin(['a', 'A', 'a', 'A', 'á', 'Á', 'b']) AS s ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s)
SETTINGS enable_analyzer = 0;

-- LIMIT BY in order is NOT used when the sort key is collated.
SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT arrayJoin(['a', 'A']) AS s ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s)
WHERE explain ILIKE '%LimitByTransform (InOrder)%';

-- LIMIT BY in order is still used without a collator.
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT arrayJoin(['a', 'A']) AS s ORDER BY s LIMIT 1 BY s)
WHERE explain ILIKE '%LimitByTransform (InOrder)%';

-- LIMIT BY in order, per stream (parallel read from a table).
SELECT count()
FROM (SELECT s FROM test ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s)
SETTINGS max_threads = 4, max_block_size = 1000;

-- Negative LIMIT BY in order + collation: LIMIT -1 BY s keeps the last row per distinct s -> 2.
SELECT count()
FROM (SELECT if(number % 2 = 0, 'a', 'A') AS s FROM numbers(1000) ORDER BY s COLLATE 'en-u-ks-level2' LIMIT -1 BY s)
SETTINGS max_block_size = 7;

-- The optimization is still applied to the value-ordered prefix before a collated column: DISTINCT
-- runs in order on `a` while deduplicating the collated `b` by value within each group.
SELECT arraySort(groupArray((a, b)))
FROM (SELECT DISTINCT a, b FROM (SELECT number % 4 AS a, ['x', 'X', 'y', 'Y'][number % 4 + 1] AS b FROM numbers(2000) ORDER BY a, b COLLATE 'en-u-ks-level2'))
SETTINGS optimize_distinct_in_order = 1, max_block_size = 13;

-- DISTINCT in order is still used for the non-collated prefix.
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM (SELECT number % 4 AS a, toString(number % 2) AS b FROM numbers(20) ORDER BY a, b COLLATE 'en-u-ks-level2') SETTINGS optimize_distinct_in_order = 1)
WHERE explain ILIKE '%DistinctSorted%';

DROP TABLE test;
