-- Tags: no-random-merge-tree-settings
-- ^ the test relies on a fixed mark layout (index_granularity = 4, 25 data marks).

DROP TABLE IF EXISTS t_ppk;

CREATE TABLE t_ppk (a UInt64, b UInt64, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 4, index_granularity_bytes = 0;

INSERT INTO t_ppk SELECT number, number FROM numbers(100);
-- Single part all_1_1_0 with 25 data marks (granule i starts at a = 4*i).

-- Analyzing the whole part through the ranges argument (partial-load path) reproduces the
-- plain full analysis.
SELECT
  (SELECT groupArray((part_name, ranges)) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a >= 40 AND a < 60))
  =
  (SELECT groupArray((part_name, ranges)) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a >= 40 AND a < 60, [('all_1_1_0', [(0, 25)])]));

-- A sub-range fully containing the match (marks ~10..14) reproduces the full result.
SELECT
  (SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a >= 40 AND a < 60) WHERE part_name = 'all_1_1_0')
  =
  (SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a >= 40 AND a < 60, [('all_1_1_0', [(8, 20)])]) WHERE part_name = 'all_1_1_0');

-- A sub-range disjoint from the match selects nothing.
SELECT empty(ranges) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a >= 40 AND a < 60, [('all_1_1_0', [(0, 5)])]) WHERE part_name = 'all_1_1_0';

-- The seek-gap merge (merge_tree_min_rows_for_seek) must not bridge the gap between the
-- requested ranges: the marks in between were not analyzed (during distributed analysis they
-- belong to other replicas). a = 5 matches mark 1, a = 85 matches mark 21; without the
-- per-input-range merge boundary the result would be a single range (1, 22).
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', a = 5 OR a = 85, [('all_1_1_0', [(0, 5), (20, 25)])]) WHERE part_name = 'all_1_1_0'
SETTINGS merge_tree_min_rows_for_seek = 1000;

-- Skip index (minmax on b): analyzing the whole part through ranges reproduces the full result,
-- so skip-index pruning is applied over the requested ranges too.
SELECT
  (SELECT groupArray((part_name, ranges)) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', b >= 40 AND b < 60))
  =
  (SELECT groupArray((part_name, ranges)) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', b >= 40 AND b < 60, [('all_1_1_0', [(0, 25)])]));

-- Argument validation.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [(0, 5)]), ('all_1_1_0', [(5, 10)])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [(1, [(0, 5)])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', 1)]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [1, 2])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [(-1, 5)])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [(5, 5)])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [(5, 3)])]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk', true, [('all_1_1_0', [(0, 26)])]); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_ppk;
