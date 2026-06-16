-- Tags: no-fasttest

-- After `ALTER TABLE ... ADD INDEX` the index exists only in metadata; parts
-- written before it have no index files. `vectorSearch` has no exact-search
-- fallback for such parts, so it must reject the query instead of silently
-- skipping the part and returning an incomplete top-K.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_unmat;

CREATE TABLE tab_unmat(id Int32, vec Array(Float32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_unmat VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]);

ALTER TABLE tab_unmat ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2);

SELECT '-- a part without index files is rejected, not silently skipped';
SELECT id FROM vectorSearch(currentDatabase(), tab_unmat, idx, [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

ALTER TABLE tab_unmat MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT '-- after MATERIALIZE INDEX the query works';
SELECT id FROM vectorSearch(currentDatabase(), tab_unmat, idx, [1.0, 0.0], 2) ORDER BY _score ASC, id;

DROP TABLE tab_unmat;
