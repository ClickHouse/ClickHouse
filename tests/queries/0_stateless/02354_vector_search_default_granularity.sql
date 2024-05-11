-- Tags: no-fasttest, no-ordinary-database

-- Tests that vector similarity indexes by default use a (non-standard) index granularity of 100 mio.

SET allow_experimental_usearch_index = 1;

-- After CREATE TABLE
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity);
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'vec_idx';

-- After ALTER TABLE
DROP TABLE tab;
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id);
ALTER TABLE tab ADD INDEX vec_idx(vec) TYPE vector_similarity;
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'vec_idx';

DROP TABLE tab;
