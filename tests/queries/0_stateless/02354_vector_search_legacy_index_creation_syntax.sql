-- Tags: no-fasttest, no-ordinary-database

-- Tests the legacy syntax to create vector similarity indexes before #70616.
-- Support for this syntax can be removed after mid-2025.

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f32', 42, 99, 113)) ENGINE = MergeTree ORDER BY id; -- Note the 6th parameter: 133

DROP TABLE tab;

