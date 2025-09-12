-- Tags: no-fasttest, no-ordinary-database

-- Tests that vector search indexes use a (non-standard) index granularity of 100 mio by default.

SET allow_experimental_vector_similarity_index = 1;

-- After CREATE TABLE
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx(vec) TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

-- After ALTER TABLE
DROP TABLE tab;
CREATE TABLE tab (id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE tab ADD INDEX idx(vec) TYPE vector_similarity('hnsw', 'L2Distance');
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

DROP TABLE tab;
