-- Tags: no-fasttest, no-ordinary-database

-- Tests that multiple vector similarity indexes can be created on the same column (even if that makes no sense)

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity('hnsw', 'L2Distance', 1));

ALTER TABLE tab ADD INDEX idx(vec) TYPE minmax;
ALTER TABLE tab ADD INDEX vec_idx1(vec) TYPE vector_similarity('hnsw', 'cosineDistance', 1);
ALTER TABLE tab ADD INDEX vec_idx2(vec) TYPE vector_similarity('hnsw', 'L2Distance', 1); -- silly but creating the same index also works for non-vector indexes ...

DROP TABLE tab;
