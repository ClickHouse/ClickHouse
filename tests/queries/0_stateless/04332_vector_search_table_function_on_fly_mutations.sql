-- Tags: no-fasttest, no-shared-catalog
-- no-shared-catalog: SYSTEM STOP MERGES and on-the-fly mutation timing differ with shared catalog

-- The scorer searches persisted index data, which does not reflect pending
-- on-the-fly updates of the indexed columns. Such parts must be rejected
-- instead of returning a top-K computed from stale vectors. Updates of
-- non-indexed columns do not affect the index and stay allowed.

SET allow_experimental_search_topk_table_functions = 1;
SET apply_mutations_on_fly = 1;
SET mutations_sync = 0;

DROP TABLE IF EXISTS tab_onfly;

CREATE TABLE tab_onfly(id Int32, flag UInt8, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_onfly VALUES (0, 0, [0.0, 0.0]), (1, 0, [1.0, 0.0]), (2, 0, [2.0, 0.0]);

SYSTEM STOP MERGES tab_onfly;

ALTER TABLE tab_onfly UPDATE flag = 1 WHERE id = 0;

SELECT '-- a pending update of a non-indexed column is fine';
SELECT id FROM vectorSearch(currentDatabase(), tab_onfly, idx, [0.0, 0.0], 2) ORDER BY _score ASC, id;

ALTER TABLE tab_onfly UPDATE vec = [9.0, 9.0] WHERE id = 0;

SELECT '-- a pending update of the indexed column rejects the search';
SELECT id FROM vectorSearch(currentDatabase(), tab_onfly, idx, [0.0, 0.0], 2); -- { serverError NOT_IMPLEMENTED }

SYSTEM START MERGES tab_onfly;

DROP TABLE tab_onfly;
