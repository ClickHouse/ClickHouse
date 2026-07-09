-- Tags: no-parallel, no-random-merge-tree-settings, no-random-settings
-- - no-parallel -- asserts per-query primary-index load counts after SYSTEM CLEAR PRIMARY INDEX
--   CACHE; a concurrent cache clear or cache pressure would change them
-- - no-random-merge-tree-settings -- relies on a fixed mark layout (index_granularity = 4,
--   25 marks) and on the primary index cache being enabled

DROP TABLE IF EXISTS t_ppk_cache;

CREATE TABLE t_ppk_cache (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 0,
         index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_ppk_cache SELECT number, number FROM numbers(100);
-- Single part all_1_1_0 with 25 marks (granule i covers a in [4*i, 4*i+4)).

SYSTEM CLEAR PRIMARY INDEX CACHE;

-- Segment [8, 12): loads index rows for marks [8, 13) - 5 rows.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a >= 41 AND a < 43, [('all_1_1_0', [(8, 12)])]) WHERE part_name = 'all_1_1_0';

-- The same segment again: served by the cached entry - no load.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a >= 41 AND a < 43, [('all_1_1_0', [(8, 12)])]) WHERE part_name = 'all_1_1_0';

-- A subset of the cached entry: covered - no load.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a = 42, [('all_1_1_0', [(9, 11)])]) WHERE part_name = 'all_1_1_0';

-- A disjoint segment [16, 20): the entry is widened - the union of marks [8, 13) and [16, 21)
-- (10 rows) is reloaded and replaces the entry.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a >= 73 AND a < 75, [('all_1_1_0', [(16, 20)])]) WHERE part_name = 'all_1_1_0';

-- Whole-part analysis: a partial entry cannot serve it - the full index (25 rows) replaces it.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a >= 41 AND a < 43) WHERE part_name = 'all_1_1_0';

-- Any segment is now covered by the full entry - no load.
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_ppk_cache', a >= 89 AND a < 91, [('all_1_1_0', [(20, 24)])]) WHERE part_name = 'all_1_1_0';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['LoadedPrimaryIndexFiles'], ProfileEvents['LoadedPrimaryIndexRows']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%FROM mergeTreeAnalyzeIndexes(%'
  AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_ppk_cache;
