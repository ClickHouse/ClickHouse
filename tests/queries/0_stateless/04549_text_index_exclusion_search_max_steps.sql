-- Tags: no-parallel-replicas
-- no-parallel-replicas: the profile events are checked via the query log of the initiator.

-- The step budget of the generic exclusion search over the text index may only change which granules
-- are read, never the query results: ranges that were not fully analyzed are accepted as a whole and
-- filtered on read.

DROP TABLE IF EXISTS t_text_index_exclusion_steps;

CREATE TABLE t_text_index_exclusion_steps
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8;

-- The token 'needle' is localized in a few rows far apart, so the exclusion search has to descend
-- into several distant subranges. The token 'common' is in every row, the token 'absent' in none.
INSERT INTO t_text_index_exclusion_steps
SELECT number, concat('common word', toString(number), if(number IN (10, 4096, 9990), ' needle', ''))
FROM numbers(10000);

OPTIMIZE TABLE t_text_index_exclusion_steps FINAL;

-- The exclusion search over the text index runs only at index-analysis time.
SET use_skip_indexes_on_data_read = 0;
SET query_plan_direct_read_from_text_index = 0;
-- The query condition cache would pre-split the ranges for repetitions of the same predicate and
-- change the number of search steps.
SET use_query_condition_cache = 0;

-- The first SELECT is the ground truth without skip indexes. Comments are not placed between the
-- echoed queries because a leading comment becomes part of the query text in the query log and
-- would break the query matching below.

-- { echoOn }

SET merge_tree_coarse_index_granularity = 8;

SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS use_skip_indexes = 0;

SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 1;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 15;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 100000;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0;

SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 15, merge_tree_coarse_index_granularity = 2;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'needle')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 15, merge_tree_coarse_index_granularity = 64;

SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'absent')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 1;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'absent')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0;

SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'common')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 1;
SELECT count(), sum(id) FROM t_text_index_exclusion_steps WHERE hasToken(s, 'common')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0;

-- { echoOff }

SYSTEM FLUSH LOGS query_log;

-- For each query above, in execution order: was the exclusion search over the text index used, and
-- did it hit the step budget? A budget too small for a matching token must hit the limit; a search
-- with an unlimited budget or over a token absent from the part must not.
SELECT
    ProfileEvents['TextIndexGenericExclusionSearchAlgorithm'] > 0,
    ProfileEvents['TextIndexGenericExclusionSearchStepLimitReached'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT count(), sum(id) FROM t_text_index_exclusion_steps%'
ORDER BY event_time_microseconds;

DROP TABLE t_text_index_exclusion_steps;
