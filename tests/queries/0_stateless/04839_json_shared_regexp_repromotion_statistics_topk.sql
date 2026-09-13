-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database, long

SET enable_json_type = 1;

DROP TABLE IF EXISTS shared_stats_topk_04839;

-- Statistics used to truncate shared_data_paths_statistics at MAX_SHARED_DATA_STATISTICS_SIZE
-- (10000) by first-encounter order, not frequency; a late-arriving hot path could never be tracked.
CREATE TABLE shared_stats_topk_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=1, SHARED REGEXP '.*')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO shared_stats_topk_04839
SELECT number AS id, ('{"cold_' || toString(number) || '":1}')::JSON(max_dynamic_paths=1) AS j FROM numbers(10000)
UNION ALL
SELECT number + 10000 AS id, '{"hot_path":1}'::JSON(max_dynamic_paths=1) AS j FROM numbers(5000);

-- Control: everything landed in one part, so the encounter order is genuinely determined by id.
SELECT 'parts', uniqExact(_part) FROM shared_stats_topk_04839;

-- Retire the broad rule and force a rewrite that reconsiders placement from statistics.
ALTER TABLE shared_stats_topk_04839 MODIFY COLUMN j JSON(max_dynamic_paths=1);
ALTER TABLE shared_stats_topk_04839 MODIFY SETTING allow_json_shared_data_paths_repromotion = 1;
OPTIMIZE TABLE shared_stats_topk_04839 FINAL;

-- The regression: with only one dynamic-path slot available, it must go to the path that
-- actually appears 5000 times, not to whichever cold (frequency-1) path was seen first.
SELECT 'winner', arraySort(JSONDynamicPaths(j)) FROM shared_stats_topk_04839 WHERE id = 10000;

DROP TABLE shared_stats_topk_04839;
