-- Tags: no-fasttest

-- `vectorSearch` must enforce the same K cap (`max_limit_for_vector_search_queries`)
-- and the same `hnsw_candidate_list_size_for_search != 0` contract as the legacy
-- `ORDER BY ... LIMIT` vector index path. Unlike that path it has no exact-search
-- fallback, so both violations are errors.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_limits;

CREATE TABLE tab_limits(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_limits VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]);

SELECT '-- K above max_limit_for_vector_search_queries is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_limits, idx, [1.0, 0.0], 11) SETTINGS max_limit_for_vector_search_queries = 10; -- { serverError BAD_ARGUMENTS }

SELECT '-- K at the cap works';
SELECT id FROM vectorSearch(currentDatabase(), tab_limits, idx, [1.0, 0.0], 2) ORDER BY _score ASC, id SETTINGS max_limit_for_vector_search_queries = 2;

SELECT '-- hnsw_candidate_list_size_for_search = 0 is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_limits, idx, [1.0, 0.0], 2) SETTINGS hnsw_candidate_list_size_for_search = 0; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab_limits;
