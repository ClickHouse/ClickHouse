-- Tags: no-parallel, no-parallel-replicas, no-release, no-old-analyzer
-- Tag no-parallel: the last check compares SelectedMarks and asserts a QueryConditionCacheHits
--                  across two separate queries on the instance-wide query condition cache; a
--                  sibling test's SYSTEM DROP QUERY CONDITION CACHE landing between them flips
--                  both, which the table_uuid scoping below cannot prevent
-- Tag no-parallel-replicas: single-node test; parallel replicas relocate index analysis and the
--                           query condition cache writes, so the per-entry counts below do not hold
-- Tag release: reads table_uuid/condition_hash/matching_marks from system.query_condition_cache,
--              which are only available in debug and sanitizer builds
-- Tag no-old-analyzer: the old analyzer never reaches the query condition cache, so there is
--                      nothing to count here

-- On a table with no effective skip index, one predicate is recorded under one cache key rather
-- than two: index analysis and the row-level filter both write the bare condition hash. With an
-- effective skip index the profiled key is kept, so skip-index exclusions stay separated from
-- row-level ones (issue #108519).

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_indexed;

SET use_query_condition_cache = 1;
-- The stress-test profile enables the server-side AST fuzzer, whose re-executions inherit
-- log_comment and are logged too, which would add rows to the per-query checks below.
SET ast_fuzzer_runs = 0;
-- PREWHERE moves the row-level write to MergeTreeSelectProcessor, which keys it on the PREWHERE
-- expression instead of the WHERE one, so the two writers would use different predicates.
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
-- One block per granule, so the row-level writer can see an entirely filtered block.
SET max_block_size = 8;

CREATE TABLE tab (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0,
         -- Automatic column statistics prune independently, which adds entries the counts below
         -- do not describe.
         auto_statistics_types = '';

CREATE TABLE tab_indexed (a UInt64, b UInt64, INDEX bx b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0,
         -- Automatic column statistics prune independently, which adds entries the counts below
         -- do not describe.
         auto_statistics_types = '';

SYSTEM STOP MERGES tab;
SYSTEM STOP MERGES tab_indexed;

INSERT INTO tab SELECT number, number % 100 FROM numbers(400);
INSERT INTO tab SELECT number + 1000, number % 100 FROM numbers(400);
INSERT INTO tab_indexed SELECT number, number % 100 FROM numbers(400);
INSERT INTO tab_indexed SELECT number + 1000, number % 100 FROM numbers(400);

-- `a > 1200` drops one part and part of another by primary key, which index analysis records;
-- `b = 7` leaves some surviving granules with no matching row, which the row-level filter records.
SELECT 'no skip index: one entry per part and predicate';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(b) FROM tab WHERE a > 1200 AND b = 7;
SELECT max(entries_per_part) FROM
(
    SELECT count() AS entries_per_part FROM system.query_condition_cache
    WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'tab')
    GROUP BY part_name
);

-- The surviving entry keeps exactly the two granules that hold a matching row, so both writers'
-- exclusions are present: index analysis zeroed the leading run and the row-level filter zeroed
-- every other surviving granule. Either exclusion alone leaves more granules matching.
SELECT max(startsWith(matching_marks, '0')), max(countMatches(matching_marks, '1'))
FROM system.query_condition_cache
WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'tab');

SELECT 'with an effective skip index: the profiled key is kept';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(b) FROM tab_indexed WHERE a > 1200 AND b = 7;
SELECT max(entries_per_part) FROM
(
    SELECT count() AS entries_per_part FROM system.query_condition_cache
    WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'tab_indexed')
    GROUP BY part_name
);

-- The index really was used, otherwise the check above would assert nothing.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(b) FROM tab_indexed WHERE a > 1200 AND b = 7)
WHERE explain ILIKE '%bx%';

-- The setting alone keeps the two keys apart, before any of the thresholds that would actually
-- start remote analysis are met. That is the conservative direction and the only part of the
-- exception this single-node test can observe: whether a remote replica really contributed an
-- exclusion is not visible here, so this check does not claim to cover that.
SELECT 'the distributed_index_analysis setting alone keeps the profiled key';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(b) FROM tab WHERE a > 1200 AND b = 7 SETTINGS distributed_index_analysis = 1;
SELECT max(entries_per_part) FROM
(
    SELECT count() AS entries_per_part FROM system.query_condition_cache
    WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'tab')
    GROUP BY part_name
);

SELECT 'the collapsed entry is applied: same rows, fewer marks, one consultation';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(b) FROM tab WHERE a > 1200 AND b = 7 SETTINGS log_comment = '04652_cold';
SELECT sum(b) FROM tab WHERE a > 1200 AND b = 7 SETTINGS log_comment = '04652_warm';
SYSTEM FLUSH LOGS query_log;
-- The second read of the same predicate reads strictly fewer marks, and reports exactly one hit for
-- the whole consultation even though two keys used to be probed.
SELECT
    argMax(ProfileEvents['SelectedMarks'], event_time_microseconds) FILTER (WHERE log_comment = '04652_warm')
    < argMax(ProfileEvents['SelectedMarks'], event_time_microseconds) FILTER (WHERE log_comment = '04652_cold'),
    argMax(ProfileEvents['QueryConditionCacheHits'], event_time_microseconds) FILTER (WHERE log_comment = '04652_warm'),
    argMax(ProfileEvents['QueryConditionCacheMisses'], event_time_microseconds) FILTER (WHERE log_comment = '04652_warm')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04652_cold', '04652_warm');

SELECT 'a different predicate is not served from the collapsed entry';
SELECT sum(b) FROM tab WHERE a > 1200 AND b = 8;

DROP TABLE tab;
DROP TABLE tab_indexed;
