-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS tab_coarse_hint;
DROP TABLE IF EXISTS tab_exact_hint;

CREATE TABLE tab_coarse_hint (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi', allow_experimental_text_index_coarse_granularity = 1;

CREATE TABLE tab_exact_hint (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi';

-- 'common' coarsens (8192 rows > budget of 32 buckets), 'rare' stays exact (25 rows).
INSERT INTO tab_coarse_hint
SELECT number, concat('common', if(number % 331 = 11, ' rare', ''))
FROM numbers(8192)
SETTINGS log_comment = '04649_insert_coarse';

INSERT INTO tab_exact_hint SELECT id, s FROM tab_coarse_hint;

SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'common');
SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'rare');

SYSTEM FLUSH LOGS query_log;

-- The insert coarsened at least one token.
SELECT 'insert coarsened tokens', ProfileEvents['TextIndexCoarsenedTokens'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04649_insert_coarse'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- A coarse index is used only as a hint: the plan reads the index virtual column
-- and keeps the original predicate, which filters out the lossy superset.
SELECT 'coarse index adds a hint', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'common')
) WHERE explain LIKE '%INPUT%\_\_text_index%';

-- A `FUNCTION hasToken` action means the predicate is still evaluated on the data.
-- Its absence is what distinguishes the exact rewrite: there the virtual column is just
-- aliased to the predicate name, so grepping for the bare `hasToken` substring would still
-- match the alias and the virtual column's default expression.
SELECT 'coarse index keeps the predicate', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'common')
) WHERE explain LIKE '%FUNCTION hasToken%';

-- Even a query over a token that stays exact in every part goes through the hint,
-- because any token of a coarse index may be lossy in some part.
SELECT 'exact token also keeps the predicate', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'rare')
) WHERE explain LIKE '%FUNCTION hasToken%';

-- An index without coarse posting lists replaces the predicate (exact direct read).
SELECT 'exact index removes the predicate', count() FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_exact_hint WHERE hasToken(s, 'common')
) WHERE explain LIKE '%FUNCTION hasToken%';

-- Without the hint setting the coarse index is not used for direct read at all.
SELECT 'no hint, no direct read', count() FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'common') SETTINGS query_plan_text_index_add_hint = 0
) WHERE explain LIKE '%INPUT%\_\_text_index%';

-- The results stay correct without the hint.
SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'common') SETTINGS query_plan_text_index_add_hint = 0;
SELECT count() FROM tab_coarse_hint WHERE hasToken(s, 'rare') SETTINGS query_plan_text_index_add_hint = 0;

DROP TABLE tab_coarse_hint;
DROP TABLE tab_exact_hint;
