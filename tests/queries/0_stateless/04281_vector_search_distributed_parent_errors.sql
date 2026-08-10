-- Tags: no-fasttest

-- Per plan 02 §step 2 engine guard: vectorSearch supports only
-- MergeTree-family source tables. A Distributed parent must surface
-- as NOT_IMPLEMENTED (use remote(...) instead — covered by 04280).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS vs_inner;
DROP TABLE IF EXISTS vs_dist;

CREATE TABLE vs_inner(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO vs_inner VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);

-- A Distributed table over the MergeTree inner.
CREATE TABLE vs_dist AS vs_inner
ENGINE = Distributed('test_shard_localhost', currentDatabase(), vs_inner);

SELECT '-- vectorSearch on a MergeTree inner: OK';
SELECT id
FROM vectorSearch(currentDatabase(), vs_inner, idx, [1.0, 0.0], 2)
ORDER BY _score, id;

SELECT '-- vectorSearch on a Distributed parent: NOT_IMPLEMENTED';
SELECT id
FROM vectorSearch(currentDatabase(), vs_dist, idx, [1.0, 0.0], 2)
ORDER BY _score, id; -- { serverError NOT_IMPLEMENTED }

DROP TABLE vs_dist;
DROP TABLE vs_inner;
