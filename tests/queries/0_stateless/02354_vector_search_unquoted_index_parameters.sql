-- Tags: no-fasttest, no-ordinary-database

SET allow_experimental_vector_similarity_index = 1;

-- Tests that quoted and unquoted parameters can be passed to vector search indexes.

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

CREATE TABLE tab1 (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity('hnsw', 'L2Distance'));
CREATE TABLE tab2 (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity(hnsw, L2Distance));

DROP TABLE tab1;
DROP TABLE tab2;

CREATE TABLE tab1 (id Int32, vec Array(Float32), PRIMARY KEY id);
CREATE TABLE tab2 (id Int32, vec Array(Float32), PRIMARY KEY id);

ALTER TABLE tab1 ADD INDEX idx1(vec) TYPE vector_similarity('hnsw', 'L2Distance');
ALTER TABLE tab2 ADD INDEX idx2(vec) TYPE vector_similarity(hnsw, L2Distance);

DROP TABLE tab1;
DROP TABLE tab2;
