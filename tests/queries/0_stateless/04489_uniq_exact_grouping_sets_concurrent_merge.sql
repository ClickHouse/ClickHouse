-- Regression test for a concurrent-merge data race in uniqExact two-level sets.
-- Merging the same uniqExact state into several GROUPING SETS destinations from
-- multiple threads used to mutate the shared source set via a const getTwoLevelSet,
-- corrupting the pointer and crashing with a segmentation fault.
-- See https://github.com/ClickHouse/ClickHouse/issues/108912

DROP TABLE IF EXISTS st_04489;

CREATE TABLE st_04489 (a UInt32, b UInt32, c UInt32, s AggregateFunction(uniqExact, UInt64)) ENGINE = MergeTree ORDER BY (a, b, c);

-- Each (a, b, c) group accumulates > 100000 distinct values, so the uniqExact sets become two-level.
INSERT INTO st_04489 SELECT number % 4, number % 3, number % 2, uniqExactState(number) FROM numbers_mt(4000000) GROUP BY 1, 2, 3;

-- Repeat a few times: the race is timing dependent but used to crash almost immediately.
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c)) SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c)) SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c)) SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY CUBE (a, b, c) SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;
SELECT a, b, c, uniqExactMerge(s) FROM st_04489 GROUP BY ROLLUP (a, b, c) SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1 FORMAT Null;

-- Result must be correct: the multi-threaded state merge has to match the same query computed single-threaded.
SELECT count() FROM
(
    SELECT a, b, c, uniqExactMerge(s) AS u FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c))
        SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_use_nulls = 1
    EXCEPT
    SELECT a, b, c, uniqExactMerge(s) AS u FROM st_04489 GROUP BY GROUPING SETS ((a), (b), (c), (a, b), (b, c))
        SETTINGS max_threads = 1, group_by_two_level_threshold = 1, group_by_use_nulls = 1
);

DROP TABLE st_04489;
