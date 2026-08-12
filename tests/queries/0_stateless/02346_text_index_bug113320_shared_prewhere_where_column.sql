-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/113320
-- A column used by both PREWHERE and a WHERE text-search predicate must not be dropped from the read set.

SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;  -- avoid randomization
SET text_index_like_min_pattern_length = 4;                 -- avoid randomization
SET text_index_like_max_postings_to_read = 50;              -- avoid randomization
SET optimize_move_to_prewhere = 0;                          -- keep each predicate in the clause it is written in

CREATE TABLE tab
(
    id UInt64,
    txt String,
    INDEX idx txt TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO tab SELECT toString(number), concat(['8Gamma', '8Delta', '7Gamma', '7Delta'][number % 4 + 1], toString(number)) FROM numbers(100);

SELECT 'LIKE in PREWHERE, ILIKE in PREWHERE';

SELECT count() FROM tab PREWHERE txt LIKE '8%' AND txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab PREWHERE txt LIKE '8%' AND txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'LIKE in PREWHERE, ILIKE in WHERE (the reported crash)';

SELECT count() FROM tab PREWHERE txt LIKE '8%' WHERE txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab PREWHERE txt LIKE '8%' WHERE txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'LIKE in WHERE, ILIKE in PREWHERE';

SELECT count() FROM tab PREWHERE txt ILIKE '%gamma%' WHERE txt LIKE '8%' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab PREWHERE txt ILIKE '%gamma%' WHERE txt LIKE '8%' SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'LIKE in WHERE, ILIKE in WHERE';

SELECT count() FROM tab WHERE txt LIKE '8%' AND txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE txt LIKE '8%' AND txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'Read column set';

SELECT trimLeft(explain)
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tab PREWHERE txt LIKE '8%' WHERE txt ILIKE '%gamma%' SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain LIKE '%Output: %' AND explain LIKE '%__text_index%';

DROP TABLE tab;
