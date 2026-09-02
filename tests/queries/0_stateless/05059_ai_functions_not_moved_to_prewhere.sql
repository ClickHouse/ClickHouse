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
-- out. Only EXPLAIN is used here, so no HTTP call is ever made.
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

SELECT 'cheap condition first';
SELECT
    countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aifilter%') AS ai_in_prewhere,
    countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab
    WHERE flag = 1 AND aiFilter(text, 'matches', map('credentials', 'ai_creds'))
);

-- The optimizer sorts conditions by its own cost estimate, so the AI condition must
-- stay out of PREWHERE however the query is written.
SELECT 'AI condition first';
SELECT
    countIf(explain ILIKE '%prewhere%' AND explain ILIKE '%aifilter%') AS ai_in_prewhere,
    countIf(explain ILIKE '%prewhere%') > 0 AS cheap_in_prewhere
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab
    WHERE aiFilter(text, 'matches', map('credentials', 'ai_creds')) AND flag = 1
);

-- With no other condition there is nothing to move, so there is no PREWHERE at all.
SELECT 'AI condition alone';
SELECT countIf(explain ILIKE '%prewhere%') AS prewhere_steps
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab
    WHERE aiFilter(text, 'matches', map('credentials', 'ai_creds'))
);

DROP NAMED COLLECTION ai_creds;
DROP TABLE tab;
