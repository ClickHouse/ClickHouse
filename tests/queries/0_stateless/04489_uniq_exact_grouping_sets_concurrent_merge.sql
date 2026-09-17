-- Regression test for a concurrent-merge data race in uniqExact two-level sets.
-- Merging the same uniqExact state into several GROUPING SETS destinations from
-- multiple threads used to mutate the shared source set via a const getTwoLevelSet,
-- corrupting the pointer and crashing with a segmentation fault.
-- See https://github.com/ClickHouse/ClickHouse/issues/108912

DROP TABLE IF EXISTS st_04489;

CREATE TABLE st_04489 (a UInt32, b UInt32, c UInt32, s AggregateFunction(uniqExact, UInt64)) ENGINE = MergeTree ORDER BY (a, b, c);

-- Each of the 12 (a, b, c) groups gets 125000 distinct values, above the 100000 uniqExact auto-two-level threshold.
INSERT INTO st_04489 SELECT number % 4, number % 3, number % 2, uniqExactState(number) FROM numbers_mt(1500000) GROUP BY 1, 2, 3;

-- The race is timing dependent but TSAN detected the problem
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c)) SETTINGS max_threads = 8, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY CUBE (a, b, c) SETTINGS max_threads = 8, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY ROLLUP (a, b, c) SETTINGS max_threads = 8, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;

DROP TABLE st_04489;
