-- Tags: no-fasttest, no-ordinary-database

-- Tests that vector similarity indexes cannot be created with index_granularity_bytes = 0

DROP TABLE IF EXISTS tab;

-- If adaptive index granularity is disabled, certain vector search queries with PREWHERE run into LOGICAL_ERRORs.
--     CREATE TABLE tab (`id` Int32, `vec` Array(Float32), INDEX idx vec TYPE  vector_similarity('hnsw', 'L2Distance') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
--     INSERT INTO tab SELECT number, [toFloat32(number), 0.] FROM numbers(10000);
--     WITH [1., 0.] AS reference_vec SELECT id, L2Distance(vec, reference_vec) FROM tab PREWHERE toLowCardinality(10) ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 100;
-- As a workaround, force enabled adaptive index granularity for now (it is the default anyways).
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 1)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
ALTER TABLE tab ADD INDEX vec_idx1(vec) TYPE vector_similarity('hnsw', 'cosineDistance', 1); -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab;
