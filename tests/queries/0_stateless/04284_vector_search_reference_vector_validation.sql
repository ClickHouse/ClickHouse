-- Tags: no-fasttest

-- `searchVectorIndex` validates the SELECT-clause reference vector before
-- handing it to USearch — dimension mismatch and `NaN`/`Inf` elements are
-- rejected with `INCORRECT_QUERY` rather than corrupting the index search.
-- The `ORDER BY ... LIMIT` vector path has the same checks in
-- `MergeTreeIndexConditionVectorSimilarity::calculateApproximateNearestNeighbors`;
-- this test pins them down on the `vectorSearch` table-function path.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id Int32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [0.0, 1.0]);

SELECT '-- dimension mismatch: query vector longer than index';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0, 0.0], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- dimension mismatch: query vector shorter than index';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- NaN element rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, nan], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- +Inf element rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, inf], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- valid query succeeds';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2) ORDER BY _score, id;

DROP TABLE tab;
