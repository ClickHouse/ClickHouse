-- Correctness test for the multi-needle predicates evaluated by the text bloom filter
-- (`tokenbf_v1`) at the granule level: `IN`, `NOT IN`, `multiSearchAny`, `hasAny` and `hasAll`.
-- These are the paths that were rewritten to short-circuit instead of materializing a per-row
-- `std::vector<bool>` in `MergeTreeConditionBloomFilterText::mayBeTrueOnGranule` (the `match`
-- with-alternatives path shares the same `std::ranges::any_of` code as `multiSearchAny`). The
-- bloom filter must never drop a granule that contains a match, so using the index must give
-- exactly the same rows as a full scan. The data is spread across several small granules so that
-- granule skipping is actually exercised.

DROP TABLE IF EXISTS t_text_bf;

CREATE TABLE t_text_bf
(
    n UInt64,
    s String,
    arr Array(String),
    INDEX idx_s s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX idx_arr arr TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY n
SETTINGS index_granularity = 4, index_granularity_bytes = 0;

INSERT INTO t_text_bf
SELECT number, concat('word', toString(number)), [concat('tok', toString(number)), 'common']
FROM numbers(20);

-- FUNCTION_IN: two needles landing in different granules; the granules in between are pruned.
SELECT 'IN';
SELECT n FROM t_text_bf WHERE s IN ('word3', 'word17') ORDER BY n
SETTINGS force_data_skipping_indices = 'idx_s';

-- FUNCTION_NOT_IN: never prunes (can_be_true stays true after negation), returns everything else.
SELECT 'NOT IN';
SELECT n FROM t_text_bf WHERE s NOT IN ('word3', 'word17') ORDER BY n;

-- FUNCTION_MULTI_SEARCH: OR over needles, short-circuits on the first present token.
SELECT 'multiSearchAny';
SELECT n FROM t_text_bf WHERE multiSearchAny(s, ['word3', 'word17']) ORDER BY n
SETTINGS force_data_skipping_indices = 'idx_s';

-- FUNCTION_HAS_ANY on Array(String): at least one element must be present.
SELECT 'hasAny';
SELECT n FROM t_text_bf WHERE hasAny(arr, ['tok3', 'tok17']) ORDER BY n
SETTINGS force_data_skipping_indices = 'idx_arr';

-- FUNCTION_HAS_ALL on Array(String): every element must be present.
SELECT 'hasAll match';
SELECT n FROM t_text_bf WHERE hasAll(arr, ['tok3', 'common']) ORDER BY n
SETTINGS force_data_skipping_indices = 'idx_arr';

-- FUNCTION_HAS_ALL with needles that never co-occur in one granule: every granule is pruned.
SELECT 'hasAll no match';
SELECT n FROM t_text_bf WHERE hasAll(arr, ['tok3', 'tok17']) ORDER BY n
SETTINGS force_data_skipping_indices = 'idx_arr';

-- Sanity check: the index-based result equals the full-scan result for the same predicate.
-- arraySort makes the comparison independent of the (parallel) row order inside groupArray.
SELECT 'index equals full scan';
SELECT
    (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE s IN ('word3', 'word17') SETTINGS use_skip_indexes = 1)
  = (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE s IN ('word3', 'word17') SETTINGS use_skip_indexes = 0),
    (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE multiSearchAny(s, ['word3', 'word17']) SETTINGS use_skip_indexes = 1)
  = (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE multiSearchAny(s, ['word3', 'word17']) SETTINGS use_skip_indexes = 0),
    (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE hasAll(arr, ['tok3', 'common']) SETTINGS use_skip_indexes = 1)
  = (SELECT arraySort(groupArray(n)) FROM t_text_bf WHERE hasAll(arr, ['tok3', 'common']) SETTINGS use_skip_indexes = 0);

DROP TABLE t_text_bf;
