-- Lazy FINAL reads the non-intersecting parts through a reading step it builds itself, and that step
-- inherits the text index read tasks of the original step while analyzing the WHERE filter alone. The
-- granule its analysis produced then reached the reader of the PREWHERE predicate's virtual column,
-- whose search query that granule was never analyzed for, and the server aborted with
-- `Query builder not found for text search query with function ...`.

SET query_plan_optimize_lazy_final = 1;         -- off by default; the bug needs it on
SET min_filtered_ratio_for_lazy_final = 0;      -- avoid fallback to regular FINAL
SET query_plan_direct_read_from_text_index = 1; -- exercise the direct read (randomized in CI)
SET use_skip_indexes = 1;                       -- ... which needs skip indexes at all
SET use_skip_indexes_if_final = 1;              -- ... and under FINAL
SET use_skip_indexes_if_final_exact_mode = 1;   -- must stay in sync with the setting above
SET use_skip_indexes_on_data_read = 0;          -- the granule then reaches the reader as a constructor argument
SET optimize_move_to_prewhere = 0;              -- keep each predicate in the clause it is written in
SET query_plan_optimize_prewhere = 1;           -- ... and let PREWHERE reach the reading step
SET enable_analyzer = 1;                        -- lazy FINAL requires the analyzer

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_reuse;

CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

INSERT INTO tab VALUES (1, 1, 'foo'), (2, 1, 'bar'), (3, 1, 'baz');
INSERT INTO tab VALUES (1, 2, 'foo_updated');
OPTIMIZE TABLE tab FINAL; -- one part, so every part is non-intersecting and lazy FINAL splits them off

SELECT 'Both optimizations under test apply';

-- 1: the PREWHERE predicate is read directly from the text index, as a virtual column
SELECT countIf(explain ILIKE '%__text_index%') > 0
FROM (EXPLAIN actions = 1, pretty = 1 SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar');

-- 1: lazy FINAL reads the non-intersecting parts without FINAL, so the reading step drops `FINAL: 1` ...
SELECT countIf(explain ILIKE '%FINAL: 1%') = 0
FROM (EXPLAIN actions = 1, pretty = 1 SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar');

-- 1: ... which the same query keeps when the optimization is off
SELECT countIf(explain ILIKE '%FINAL: 1%') > 0
FROM (EXPLAIN actions = 1, pretty = 1 SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar' SETTINGS query_plan_optimize_lazy_final = 0);

SELECT 'Different text search predicates in PREWHERE and WHERE';

SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar';                        -- 0: no row holds both values
SELECT count() FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar';                           -- 0: the virtual column is read for the filter alone
SELECT count(str) FROM tab FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar';  -- 1: 'bar' passes both
SELECT str FROM tab FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar';         -- bar: and it is that row

-- 1: two text search predicates in one PREWHERE, only one of them known to the WHERE filter's analysis
SELECT count(str) FROM tab FINAL PREWHERE str = 'bar' AND hasAnyTokens(str, ['bar']) WHERE str = 'bar';

SELECT 'The same, with the index granule read on data read';

-- Reading the index on data read needs `max_rows_to_read` unset: a `throw` row limit disables that
-- route outright, and the stateless test profile sets one.
SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar'
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;                               -- 0
SELECT count(str) FROM tab FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar'
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;                               -- 1

SELECT 'Several read tasks of one part reuse the reader';

CREATE TABLE tab_reuse
(
    id UInt64,
    version UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 8; -- 8-row marks, so one part holds enough marks to be cut into many read tasks

INSERT INTO tab_reuse SELECT number, 1, ['foo', 'bar', 'baz', 'foo'][number % 4 + 1] FROM numbers(4096);
INSERT INTO tab_reuse VALUES (0, 2, 'foo_updated');
OPTIMIZE TABLE tab_reuse FINAL;

-- 1: one part of at least 512 marks, so the settings below really do yield several read tasks
SELECT count() = 1 AND min(rows) = 4096 AND min(marks) >= 512
FROM system.parts WHERE database = currentDatabase() AND table = 'tab_reuse' AND active;

-- 1: the PREWHERE predicate is read from the text index on this route too
SELECT countIf(explain ILIKE '%__text_index%') > 0
FROM (EXPLAIN actions = 1, pretty = 1 SELECT count(str) FROM tab_reuse FINAL PREWHERE str = 'baz' WHERE str = 'bar'
      SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0);

-- `max_threads` above 1 picks the read pool that cuts a part into several tasks, and is randomized
-- in CI. `merge_tree_min_rows_for_concurrent_read` defaults far above this part's row count, so
-- without it there would be one task. The prefetched pool creates a reader per task instead of
-- moving one between them, so it is off here.
SELECT count(str) FROM tab_reuse FINAL PREWHERE str = 'baz' WHERE str = 'bar'
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0,
         max_threads = 2, merge_tree_min_rows_for_concurrent_read = 8, allow_prefetched_read_pool_for_remote_filesystem = 0;       -- 0
SELECT count(str) FROM tab_reuse FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar'
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0,
         max_threads = 2, merge_tree_min_rows_for_concurrent_read = 8, allow_prefetched_read_pool_for_remote_filesystem = 0;       -- 1024

SELECT count(str) FROM tab_reuse FINAL PREWHERE str = 'baz' WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;                          -- 0
SELECT count(str) FROM tab_reuse FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;                          -- 1024

SELECT 'The counts do not depend on the optimizations';

SELECT count(str) FROM tab FINAL PREWHERE str = 'baz' WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;        -- 0
SELECT count(str) FROM tab FINAL PREWHERE hasAnyTokens(str, ['bar', 'baz']) WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;        -- 1
SELECT count(str) FROM tab FINAL PREWHERE str = 'bar' AND hasAnyTokens(str, ['bar']) WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;        -- 1

SELECT 'The same predicate in both clauses hashes to one search query and always worked';

SELECT count(str) FROM tab FINAL PREWHERE str = 'bar' WHERE str = 'bar';                        -- 1
SELECT count(str) FROM tab FINAL PREWHERE str = 'bar' WHERE str = 'bar'
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_direct_read_from_text_index = 0;        -- 1

DROP TABLE tab;
DROP TABLE tab_reuse;
