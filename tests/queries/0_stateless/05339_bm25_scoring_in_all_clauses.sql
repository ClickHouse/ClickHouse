-- Tags: no-parallel-replicas

-- `bm25()` can be used in any clause of the query reading the table with the scoring text index, also in a filter:
-- a condition on the score adds nothing to the score and is not part of the conjunctions masking the scores.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_clauses;
DROP TABLE IF EXISTS tab_bm25_clauses_scores;
DROP TABLE IF EXISTS tab_bm25_clauses_qcc;

CREATE TABLE tab_bm25_clauses
(
    id UInt32,
    author String,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_clauses VALUES
    (1, 'a', 'apple banana'),
    (2, 'a', 'apple apple apple'),
    (3, 'b', 'banana cherry'),
    (4, 'b', 'cherry apple banana apple'),
    (5, 'c', 'durian'),
    (6, 'c', 'apple'),
    (7, 'a', 'banana banana'),
    (8, 'b', 'apple cherry durian elderberry fig');

SELECT '-- SELECT';
CREATE TABLE tab_bm25_clauses_scores ENGINE = Memory AS
SELECT id, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']);

SELECT id, round(score, 4) FROM tab_bm25_clauses_scores ORDER BY id;

SELECT '-- WHERE';
SELECT id FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() > 1 ORDER BY id;
SELECT id FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() > 1 ORDER BY id SETTINGS optimize_move_to_prewhere = 0;
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses WHERE bm25() <= 1 AND hasAnyTokens(body, ['apple', 'banana']) ORDER BY bm25() DESC;

SELECT '-- the filter agrees with the scores of the SELECT list';
SELECT
    (SELECT groupArraySorted(100)(id) FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() BETWEEN 0.7 AND 1.2)
    = (SELECT groupArraySorted(100)(id) FROM tab_bm25_clauses_scores WHERE score BETWEEN 0.7 AND 1.2);

SELECT '-- a condition on the score in a nested conjunction is not part of its mask';
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses
WHERE (hasAnyTokens(body, ['apple', 'banana']) AND bm25() > 1) OR hasToken(body, 'durian')
ORDER BY id;
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses
WHERE (hasToken(body, 'apple') AND hasToken(body, 'banana') AND bm25() > 1.2) OR hasToken(body, 'durian')
ORDER BY id;

SELECT '-- the scores without the conditions on the score';
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses
WHERE hasAnyTokens(body, ['apple', 'banana']) OR hasToken(body, 'durian')
ORDER BY id;
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses
WHERE (hasToken(body, 'apple') AND hasToken(body, 'banana')) OR hasToken(body, 'durian')
ORDER BY id;

SELECT '-- WHERE with ORDER BY bm25() LIMIT and the dynamic top-k filter';
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses
WHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() < 1.2
ORDER BY bm25() DESC LIMIT 2
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM tab_bm25_clauses
    WHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() < 1.2
    ORDER BY bm25() DESC LIMIT 2
    SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000
)
WHERE explain LIKE '%__topKFilter%';

SELECT '-- PREWHERE';
SELECT id FROM tab_bm25_clauses PREWHERE hasAnyTokens(body, ['apple', 'banana']) WHERE bm25() > 1 ORDER BY id;
SELECT id FROM tab_bm25_clauses PREWHERE hasAnyTokens(body, ['apple', 'banana']) AND bm25() > 1 ORDER BY id;
SELECT id, round(bm25(), 4) FROM tab_bm25_clauses PREWHERE bm25() > 1 WHERE hasAnyTokens(body, ['apple', 'banana']) ORDER BY id;

SELECT '-- HAVING and QUALIFY without aggregation';
SELECT id FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) HAVING bm25() > 1 ORDER BY id;
SELECT id FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) QUALIFY bm25() > 1 ORDER BY id;

SELECT '-- GROUP BY, aggregate functions and HAVING';
SELECT author, count(), round(max(bm25()), 4), round(sum(bm25()), 4)
FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])
GROUP BY author ORDER BY author;
SELECT author FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) GROUP BY author HAVING max(bm25()) > 1.2 ORDER BY author;
SELECT bm25() > 1 AS high, count() FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) GROUP BY high ORDER BY high;

SELECT '-- WINDOW and QUALIFY';
SELECT id, author, row_number() OVER w AS rank
FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])
WINDOW w AS (PARTITION BY author ORDER BY bm25() DESC)
ORDER BY id;
SELECT id, author FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])
QUALIFY row_number() OVER (PARTITION BY author ORDER BY bm25() DESC) = 1
ORDER BY id;

SELECT '-- LIMIT BY';
SELECT author, id FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) ORDER BY bm25() DESC LIMIT 1 BY author;

SELECT '-- subquery and WITH alias';
SELECT id, round(score, 4) FROM (SELECT id, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])) WHERE score > 1 ORDER BY score DESC;
-- The condition pushed down into the subquery stays a filter step of its own above the one computing the score.
SELECT id, round(score, 4) FROM (SELECT id, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])) WHERE score > 1 ORDER BY score DESC SETTINGS query_plan_merge_filters = 0;
-- The condition on the score is also a column of the result.
SELECT id, score > 1 FROM (SELECT id, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])) WHERE score > 1 ORDER BY id;
SELECT id, score > 1 FROM (SELECT id, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])) WHERE score > 1 ORDER BY id SETTINGS query_plan_merge_filters = 0;
SELECT author, round(avg(score), 4) FROM (SELECT author, bm25() AS score FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana'])) GROUP BY author ORDER BY author;
WITH bm25() AS score SELECT id, round(score, 4) FROM tab_bm25_clauses WHERE hasAnyTokens(body, ['apple', 'banana']) AND score > 1 ORDER BY score DESC;

SELECT '-- a condition on the score does not use the query condition cache';
CREATE TABLE tab_bm25_clauses_qcc
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_clauses_qcc VALUES (1, 'apple'), (2, 'apple banana'), (3, 'banana'), (4, 'apple apple');

SELECT id FROM tab_bm25_clauses_qcc WHERE hasToken(body, 'apple') AND bm25() > 1 ORDER BY id
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

-- More rows without the token make it rarer, which raises the scores of the rows of the first part.
INSERT INTO tab_bm25_clauses_qcc SELECT number + 100, 'cherry' FROM numbers(50);

SELECT id FROM tab_bm25_clauses_qcc WHERE hasToken(body, 'apple') AND bm25() > 1 ORDER BY id
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

DROP TABLE tab_bm25_clauses;
DROP TABLE tab_bm25_clauses_scores;
DROP TABLE tab_bm25_clauses_qcc;
