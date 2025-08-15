-- Tags: no-fasttest, no-ordinary-database

-- Tests that multiple vector similarity indexes can be created on the same column (even if that makes no sense)

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity('hnsw', 'L2Distance'));

ALTER TABLE tab ADD INDEX idx(vec) TYPE minmax;
ALTER TABLE tab ADD INDEX vec_idx1(vec) TYPE vector_similarity('hnsw', 'cosineDistance');
ALTER TABLE tab ADD INDEX vec_idx2(vec) TYPE vector_similarity('hnsw', 'L2Distance'); -- silly but creating the same index also works for non-vector indexes ...

DROP TABLE tab;
