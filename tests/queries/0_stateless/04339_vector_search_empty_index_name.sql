-- Tags: no-fasttest

-- The 5-argument form of `vectorSearch` promises an explicit index name.
-- An empty name must be rejected: `executeImpl` distinguishes the arities
-- by `source_index_name.empty()`, so an empty string would silently take
-- the 4-argument auto-resolve branch (and the same query would change
-- behavior when a second `vector_similarity` index is added).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_empty_idx_name;

CREATE TABLE tab_empty_idx_name(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_empty_idx_name VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);

SELECT '-- empty index_name in the 5-arg form is rejected, not auto-resolved';
SELECT id FROM vectorSearch(currentDatabase(), tab_empty_idx_name, '', [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- the explicit and auto-resolve forms still work';
SELECT id FROM vectorSearch(currentDatabase(), tab_empty_idx_name, idx, [1.0, 0.0], 2) ORDER BY _score, id;
SELECT id FROM vectorSearch(currentDatabase(), tab_empty_idx_name, [1.0, 0.0], 2) ORDER BY _score, id;

DROP TABLE tab_empty_idx_name;
