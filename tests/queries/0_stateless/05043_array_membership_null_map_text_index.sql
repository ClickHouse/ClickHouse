-- Tags: no-parallel-replicas
-- A text index answers hasAll and hasAny with its own evaluator, so it must agree with the
-- ordinary array path on an array whose NULL element hides a value equal to the needle.

SET enable_full_text_index = 1;
-- The query condition cache is keyed on the bare condition hash as well as on one salted with
-- the skip-index profile, so the index-off arms below would prime entries the indexed arms then
-- reuse, letting an index that declines still report a pruned mark count.
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_null_map_text;

CREATE TABLE t_null_map_text
(
    id UInt32,
    arr Array(Nullable(String)),
    INDEX idx arr TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

-- id 1 stores 'zz' underneath a NULL, so it does not contain 'zz'. id 2 really contains it.
INSERT INTO t_null_map_text SELECT 1, arrayMap(x -> nullIf(x, 'zz'), ['a', 'zz', 'c']);
INSERT INTO t_null_map_text SELECT 2, ['a', 'zz', 'c'];

SELECT '-- has(), which is the expected answer for every arm below --';
SELECT id, has(arr, 'zz') FROM t_null_map_text ORDER BY id;

SELECT '-- hasAll and hasAny without the index --';
SELECT id, hasAll(arr, ['zz']), hasAny(arr, ['zz'])
FROM t_null_map_text ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- matching ids, index off --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id SETTINGS use_skip_indexes = 0;

-- query_plan_direct_read_from_text_index selects which of the two modes below runs, and the
-- test runner randomizes it, so both arms pin it explicitly. Each mode is also asserted on
-- the plan rather than on results alone: post-fix every path returns the same row, so a
-- result-only check would still pass if index evaluation silently declined.
SELECT '-- matching ids, index on, reading through the index --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;
SELECT countIf(explain ILIKE '%__text_index%') > 0 AS reads_through_index
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz'])
      SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1);
-- hasAny reaches the index through a different RPN element and evaluator than hasAll, so it
-- needs its own plan proof. The mode token pins which element: the condition reads Any only
-- for the one-query hasAny element, and All for the per-element fallback and for hasAll.
SELECT countIf(explain ILIKE '%__text_index_idx_hasAny%') > 0 AS reads_through_index,
       countIf(explain ILIKE '%mode: Any%') > 0 AS any_tokens_element
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz'])
      SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1);

SELECT '-- matching ids, index on, pruning only --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT countIf(explain ILIKE '%Granules: 1/2%') > 0 AS prunes_a_granule,
       countIf(explain ILIKE '%__text_index%') > 0 AS reads_through_index
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz'])
      SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0);
SELECT countIf(explain ILIKE '%Granules: 1/2%') > 0 AS prunes_a_granule,
       countIf(explain ILIKE '%__text_index%') > 0 AS reads_through_index,
       countIf(explain ILIKE '%mode: Any%') > 0 AS any_tokens_element
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz'])
      SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0);

-- Negative control for both probes above: with the index off neither token may appear, so a
-- token that happens to match something unrelated in the plan text cannot read as a pass.
SELECT '-- plan-shape negative control, index off --';
SELECT countIf(explain ILIKE '%Name: idx%') > 0 AS index_used,
       countIf(explain ILIKE '%__text_index%') > 0 AS reads_through_index,
       countIf(explain ILIKE '%Granules: 1/2%') > 0 AS prunes_a_granule
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz'])
      SETTINGS use_skip_indexes = 0);
SELECT countIf(explain ILIKE '%Name: idx%') > 0 AS index_used,
       countIf(explain ILIKE '%__text_index_idx_hasAny%') > 0 AS reads_through_index,
       countIf(explain ILIKE '%Granules: 1/2%') > 0 AS prunes_a_granule,
       countIf(explain ILIKE '%mode: Any%') > 0 AS any_tokens_element
FROM (EXPLAIN indexes = 1 SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz'])
      SETTINGS use_skip_indexes = 0);

SELECT '-- control: a needle that is genuinely present in both rows --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['a']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['a']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_null_map_text;
