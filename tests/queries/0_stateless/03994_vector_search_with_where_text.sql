-- Tags: no-fasttest

-- Testing the vectorSearch table function combined with hasAllTokens / hasAnyTokens
-- on a text-indexed column. This is the migration path from the removed
-- mergeTreeHybridSearch table function (PR #51576).

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

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (0, [1.0, 0.0, 0.0], 'the quick brown fox'),
    (1, [1.1, 0.0, 0.0], 'the lazy dog'),
    (2, [0.0, 2.0, 0.0], 'quick brown dog'),
    (3, [0.0, 0.0, 3.0], 'the fast car'),
    (4, [0.5, 0.5, 0.5], 'the quick rabbit');

SELECT '-- 1. Basic hybrid search: top 3 nearest to [1.0, 0.0, 0.0] with text "quick", mode all';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 3)
WHERE hasAllTokens(doc, ['quick'])
ORDER BY _score, id;

SELECT '-- 2. Hybrid search with mode "any": "quick" OR "dog"';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE hasAnyTokens(doc, ['quick', 'dog'])
ORDER BY _score, id;

SELECT '-- 3. Hybrid search with mode "all": "quick" AND "brown"';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE hasAllTokens(doc, ['quick', 'brown'])
ORDER BY _score, id;

SELECT '-- 4. Text filter matching no rows: returns empty';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE hasAllTokens(doc, ['nonexistentword'])
ORDER BY _score, id;

SELECT '-- 5. All columns are returned';
SELECT id, vec, doc, _score, _part
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 2)
WHERE hasAllTokens(doc, ['quick'])
ORDER BY _score, id;

DROP TABLE tab;

SELECT '-- 6. Multi-part test';

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    doc String,
    INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000,
    INDEX txt_idx doc TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

SYSTEM STOP MERGES tab;

-- Insert into separate parts.
INSERT INTO tab VALUES (0, [1.0, 0.0], 'hello world'), (1, [1.1, 0.0], 'hello there');
INSERT INTO tab VALUES (2, [0.0, 1.0], 'hello world'), (3, [0.0, 1.1], 'goodbye world');

SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0], 3)
WHERE hasAllTokens(doc, ['hello'])
ORDER BY _score, id;

SELECT '-- 7. Part name filtering with vectorSearch + WHERE text';
SELECT id, _score, _part
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0], 10)
WHERE _part = 'all_1_1_0' AND hasAllTokens(doc, ['hello'])
ORDER BY _score, id;

DROP TABLE tab;

SELECT '-- 8. Error: wrong vector index type';
CREATE TABLE tab (id Int32, vec Array(Float32), doc String, INDEX vec_idx doc TYPE minmax, INDEX txt_idx doc TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY id;
SELECT * FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0], 1) WHERE hasAllTokens(doc, ['hello']); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab;

SELECT '-- 9. Mixed AND with non-text filter';
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

SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, vec_idx, [1.0, 0.0, 0.0], 5)
WHERE hasAllTokens(doc, ['quick']) AND id < 4
ORDER BY _score, id;

DROP TABLE tab;
