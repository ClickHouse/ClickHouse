-- Tags: no-object-storage
-- no-object-storage since the output of the pipeline depends on the read method.

-- Verify that the trivial-count optimization correctly handles
-- `additional_table_filters`:
--   - DISABLED when the setting targets THIS table.
--   - STILL FIRES when the setting targets a different table only
--     (previously the pessimistic outer guard disabled the optimization
--     whenever any entry was present, regardless of the table).
--
-- The fix moved `parseAdditionalFilterAstIfNeeded` earlier in
-- PlannerJoinTree so `applyTrivialCountIfPossible` sees the
-- already-matched per-table AST.
--
-- Trivial-count signature in the pipeline:
--   (ReadFromPreparedSource) / SourceFromSingleChunk -> fired
--   (ReadFromMergeTree)      / MergeTreeSelect       -> did not fire

DROP TABLE IF EXISTS t_trivial_count_filter;
CREATE TABLE t_trivial_count_filter (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_trivial_count_filter SELECT number FROM numbers(100);

SET enable_analyzer = 1;
SET optimize_trivial_count_query = 1;

-- Baseline: no filter at all, trivial-count fires.
SELECT 'baseline';
SELECT explain FROM (EXPLAIN PIPELINE SELECT count() FROM t_trivial_count_filter)
WHERE explain LIKE '%ReadFromPreparedSource%' OR explain LIKE '%ReadFromMergeTree%'
   OR explain LIKE '%SourceFromSingleChunk%' OR explain LIKE '%MergeTreeSelect%';

-- Filter targets THIS table: trivial-count is disabled.
SELECT 'filtered_this_table';
SELECT explain FROM (EXPLAIN PIPELINE SELECT count() FROM t_trivial_count_filter)
WHERE explain LIKE '%ReadFromPreparedSource%' OR explain LIKE '%ReadFromMergeTree%'
   OR explain LIKE '%SourceFromSingleChunk%' OR explain LIKE '%MergeTreeSelect%'
SETTINGS additional_table_filters = {'t_trivial_count_filter': 'id < 5'};

-- Filter targets a DIFFERENT table only: trivial-count must still fire.
-- Without the fix the optimization was disabled whenever any
-- additional_table_filters entry was present.
SELECT 'filter_other_table';
SELECT explain FROM (EXPLAIN PIPELINE SELECT count() FROM t_trivial_count_filter)
WHERE explain LIKE '%ReadFromPreparedSource%' OR explain LIKE '%ReadFromMergeTree%'
   OR explain LIKE '%SourceFromSingleChunk%' OR explain LIKE '%MergeTreeSelect%'
SETTINGS additional_table_filters = {'some_other_table': 'id < 5'};

DROP TABLE t_trivial_count_filter;
