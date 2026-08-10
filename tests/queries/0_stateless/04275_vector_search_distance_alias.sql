-- Tags: no-fasttest

-- vectorSearch publishes the index distance as `_score`. For compatibility
-- with the legacy column name, `_distance` is exposed as an alias for `_score`:
-- referencing it (in the select list, WHERE, or ORDER BY) resolves to `_score`.
-- Being an alias, it does not appear in `SELECT *`.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]);

SELECT '-- _distance equals _score';
SELECT id, _score, _distance, _score = _distance AS equal
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2)
ORDER BY _score, id;

SELECT '-- ORDER BY _distance works (same as ORDER BY _score)';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2)
ORDER BY _distance, id;

SELECT '-- WHERE on _distance works';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE _distance = 0
ORDER BY id;

SELECT '-- _distance is an alias, so it does not appear in SELECT *';
SELECT * FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2) ORDER BY id;

DROP TABLE tab;
