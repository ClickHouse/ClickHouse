-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- `LIKE` and `ILIKE` resolve the pattern by scanning the index dictionary, which yields the exact
-- set of matching tokens. That makes the direct read exact for a regular index, but not for an index
-- with coarse posting lists: those store bucket ids and expand to a lossy superset of the rows, so
-- the original predicate must be kept.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS tab_coarse_like;

CREATE TABLE tab_coarse_like (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi', allow_experimental_text_index_coarse_granularity = 1;

-- 'common' occurs in 8167 rows and coarsens (its posting list exceeds the budget of 32 buckets).
-- The remaining 25 rows hold 'alpha' and are spread over the same buckets, so a lossy read of the
-- posting list of 'common' returns them as false positives.
INSERT INTO tab_coarse_like SELECT number, if(number % 331 = 11, 'alpha', 'common') FROM numbers(8192);

SELECT 'token is coarsened', countIf(coarse_level > 0) > 0 FROM mergeTreeTextIndex(currentDatabase(), tab_coarse_like, idx);

SELECT 'like', count() FROM tab_coarse_like WHERE s LIKE '%ommo%';
SELECT 'ilike', count() FROM tab_coarse_like WHERE s ILIKE '%OMMO%';
SELECT 'not like', count() FROM tab_coarse_like WHERE s NOT LIKE '%ommo%';

-- The same results without the dictionary scan and without direct read.
SELECT 'like, no dictionary scan', count() FROM tab_coarse_like WHERE s LIKE '%ommo%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'like, no direct read', count() FROM tab_coarse_like WHERE s LIKE '%ommo%' SETTINGS query_plan_direct_read_from_text_index = 0;

-- The plan keeps the predicate: a `FUNCTION like` action means it is still evaluated on the data.
SELECT 'like keeps the predicate', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_like WHERE s LIKE '%ommo%'
) WHERE explain LIKE '%FUNCTION like%';

SELECT 'ilike keeps the predicate', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_like WHERE s ILIKE '%OMMO%'
) WHERE explain LIKE '%FUNCTION ilike%';

DROP TABLE tab_coarse_like;
