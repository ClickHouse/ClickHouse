-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops a global named collection
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- An AI function issues one request per row, so it must not be moved to PREWHERE:
-- PREWHERE conditions are evaluated on every row read, while a condition left in
-- WHERE only sees the rows that survived the cheaper conditions.
--
-- `MergeTreeWhereOptimizer` prices a condition by the size of the columns it
-- reads, which cannot express that cost, so `isExpensive` keeps such a condition
-- out. Every AI function is listed here: `aiEmbed` and `aiSimilarity` do not
-- share a base class with the rest, so a new function can miss the trait.
--
-- Only EXPLAIN is used, so no HTTP call is ever made. Each case reports whether
-- the AI function reached PREWHERE (must be 0) and whether the cheap condition
-- still did (must be 1).
-- =============================================================================

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (flag UInt8, text String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT number % 2, 'row ' || toString(number) FROM numbers(16);

DROP NAMED COLLECTION IF EXISTS ai_creds;
CREATE NAMED COLLECTION ai_creds AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'test-key';

DROP NAMED COLLECTION IF EXISTS ai_vec_creds;
CREATE NAMED COLLECTION ai_vec_creds AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/embeddings',
    api_key = 'test-key';

-- The assertions describe the outcome of the WHERE-to-PREWHERE move, so the settings that
-- decide whether it runs at all are pinned rather than left to test randomization. On the
-- legacy analyzer the move happens in `InterpreterSelectQuery` instead, where the condition
-- carries no resolved function to ask, and the AI function is still moved.
SET optimize_move_to_prewhere = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_analyzer = 1;

SELECT 'aiGenerate' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aigenerate%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiGenerate(text, map('credentials', 'ai_creds')) != '');

SELECT 'aiClassify' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aiclassify%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiClassify(text, ['a', 'b'], map('credentials', 'ai_creds')) = 'a');

SELECT 'aiExtract' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aiextract%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiExtract(text, 'the topic', map('credentials', 'ai_creds')) != '');

SELECT 'aiTranslate' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aitranslate%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiTranslate(text, 'French', map('credentials', 'ai_creds')) != '');

SELECT 'aiFilter' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aifilter%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiFilter(text, 'matches', map('credentials', 'ai_creds')));

SELECT 'aiRedact' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%airedact%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiRedact(text, ['email'], map('credentials', 'ai_creds')) != '');

SELECT 'aiEmbed' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aiembed%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND length(aiEmbed(text, 'test-model', map('credentials', 'ai_vec_creds'))) != 0);

SELECT 'aiSimilarity' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aisimilarity%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND aiSimilarity(text, 'reference', 'test-model', map('credentials', 'ai_vec_creds')) != 0);

-- The optimizer sorts conditions by its own cost estimate, so the AI condition must
-- stay out of PREWHERE however the query is written.
SELECT 'AI condition written first' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aifilter%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE aiFilter(text, 'matches', map('credentials', 'ai_creds')) AND flag = 1);

-- A lambda body is not among the condition's children, so the capture has to report the
-- trait on behalf of the AI call it carries. EXPLAIN prints the lambda as `Capture[...]`
-- without the inner name, hence matching the higher-order function instead.
SELECT 'AI inside a lambda' AS fn, countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%arrayexists%') AS in_prewhere, countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE flag = 1 AND arrayExists(x -> aiFilter(x, 'matches', map('credentials', 'ai_creds')), [text]));

-- With no other condition there is nothing to move, so there is no PREWHERE at all.
SELECT 'AI condition alone' AS fn, countIf(explain ILIKE '%prewhere%') AS prewhere_lines
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE aiFilter(text, 'matches', map('credentials', 'ai_creds')));

DROP NAMED COLLECTION ai_creds;
DROP NAMED COLLECTION ai_vec_creds;
DROP TABLE tab;
