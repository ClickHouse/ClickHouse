-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 16;

-- { echo }

-- Two LIMIT BY keys reference the same sorting-key column (`a` directly and via the strict-monotonic
-- `negate(a)`). They collapse onto the single column `a`: only one can claim the prefix position, so the
-- matched order does not cover all keys and the optimization bails to the hash LimitByTransform. The
-- result must stay correct (one row per distinct `a`, since `negate(a)` adds no distinction).
DROP TABLE IF EXISTS test_collapse_single;
CREATE TABLE test_collapse_single (a Int32, v UInt32) ENGINE = MergeTree ORDER BY (a);
SYSTEM STOP MERGES test_collapse_single;
INSERT INTO test_collapse_single SELECT number % 50, number      FROM numbers(1000);
INSERT INTO test_collapse_single SELECT number % 50, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a FROM test_collapse_single LIMIT 1 BY a, negate(a) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_collapse_single) FROM (SELECT a FROM test_collapse_single LIMIT 1 BY a, negate(a) SETTINGS optimize_limit_by_in_order = 1);
DROP TABLE test_collapse_single;

-- Same collapse alongside a genuine extra key: keys (a, negate(a), b) over ORDER BY (b, a). `a` and
-- `negate(a)` compete for the single `a` column, leaving one key unmatched, so the matched order covers
-- only 2 of the 3 keys and it bails.
DROP TABLE IF EXISTS test_collapse_extra;
CREATE TABLE test_collapse_extra (a Int32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (b, a);
SYSTEM STOP MERGES test_collapse_extra;
INSERT INTO test_collapse_extra SELECT number % 50, intDiv(number, 50) % 4, number      FROM numbers(1000);
INSERT INTO test_collapse_extra SELECT number % 50, intDiv(number, 50) % 4, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b FROM test_collapse_extra LIMIT 1 BY a, negate(a), b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact((a, b)) FROM test_collapse_extra) FROM (SELECT a, b FROM test_collapse_extra LIMIT 1 BY a, negate(a), b SETTINGS optimize_limit_by_in_order = 1);
-- Contrast: dropping the redundant negate(a) leaves (a, b) as a full (permuted) cover of (b, a), so it streams.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b FROM test_collapse_extra LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
DROP TABLE test_collapse_extra;
