-- Tags: no-fasttest, no-ordinary-database

-- Tests that various conditions are checked during creation of vector similarity indexes.

SET allow_experimental_usearch_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Two or six index arguments';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity()); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('cant_have_one_arg')); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('cant', 'have', 'three_args')); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('cant', 'have', 'more', 'than', 'six', 'args', '.')); -- { serverError INCORRECT_QUERY }

SELECT '1st argument (method) must be String and hnsw';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity(3, 'L2Distance')); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('not_hnsw', 'L2Distance')); -- { serverError INCORRECT_DATA }

SELECT '2nd argument (distance function) must be String and L2Distance or cosineDistance';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 3)); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'invalid_distance')); -- { serverError INCORRECT_DATA }

SELECT '3nd argument (quantization), if given, must be String and f32, f16, ...';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 1, 1, 1, 1)); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'invalid', 2, 1, 1)); -- { serverError INCORRECT_DATA }
SELECT '4nd argument (M), if given, must be UInt64 and > 1';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 'invalid', 1, 1)); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 1, 1, 1)); -- { serverError INCORRECT_DATA }
SELECT '5nd argument (ef_construction), if given, must be UInt64 and > 0';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 'invalid', 1)); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 0, 1)); -- { serverError INCORRECT_DATA }
SELECT '6nd argument (ef_search), if given, must be UInt64 and > 0';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 1, 'invalid')); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 1, 0)); -- { serverError INCORRECT_DATA }

SELECT 'Must be created on single column';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx (vec, id) TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on Array(Float32) columns';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tab(id Int32, vec UInt64, PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Float32, PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Array(Float64), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec LowCardinality(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Nullable(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance')); -- { serverError ILLEGAL_COLUMN }

SELECT 'Rejects INSERTs of Arrays with different sizes';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance'));
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;
