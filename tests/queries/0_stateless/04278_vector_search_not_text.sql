-- Tags: no-fasttest

-- Per plan 02 §step 5: WHERE clauses on vectorSearch support negative
-- text predicates (NOT hasAllTokens / NOT hasAnyTokens). The bitmap
-- subquery just walks the same expression.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    doc String,
    INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 100000000,
    INDEX txt_idx doc TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO tab VALUES
    (0, [1.0, 0.0, 0.0], 'the quick brown fox'),
    (1, [1.1, 0.0, 0.0], 'the lazy dog'),
    (2, [0.0, 2.0, 0.0], 'quick brown dog'),
    (3, [0.0, 0.0, 3.0], 'the fast car'),
    (4, [0.5, 0.5, 0.5], 'the quick rabbit');

SELECT '-- NOT hasAllTokens: exclude docs containing "quick"';
SELECT id
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE NOT hasAllTokens(doc, ['quick'])
ORDER BY _score, id;

SELECT '-- NOT hasAnyTokens: exclude docs containing either "quick" or "lazy"';
SELECT id
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE NOT hasAnyTokens(doc, ['quick', 'lazy'])
ORDER BY _score, id;

SELECT '-- Mixed AND: contains "the" but NOT "quick"';
SELECT id
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE hasAllTokens(doc, ['the']) AND NOT hasAllTokens(doc, ['quick'])
ORDER BY _score, id;

DROP TABLE tab;
