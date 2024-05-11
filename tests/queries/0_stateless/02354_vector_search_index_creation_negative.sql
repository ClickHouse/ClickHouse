-- Tags: no-fasttest, no-ordinary-database

-- Tests that various conditions are checked during creation of vector similarity indexes.

SET allow_experimental_usearch_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'At most two index arguments';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('too', 'many', 'args')); -- { serverError INCORRECT_QUERY }

SELECT '1st argument (distance function) must be String';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity(3)); -- { serverError INCORRECT_QUERY }

SELECT 'Unsupported distance functions are rejected';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('invalid_distance_function')); -- { serverError INCORRECT_DATA }

SELECT '2nd argument (scalar kind) must be String';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity(3)); -- { serverError INCORRECT_QUERY }

SELECT 'Unsupported scalar kinds are rejected';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('L2Distance', 'invalid_kind')); -- { serverError INCORRECT_DATA }

SELECT 'Must be created on single column';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx (vec, id) TYPE vector_similarity()); -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on Array(Float32) columns';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tab(id Int32, vec Float32, PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity()); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Array(Float64), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity()); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec LowCardinality(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity()); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Nullable(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity()); -- { serverError ILLEGAL_COLUMN }

SELECT 'Rejects INSERTs of Arrays with different sizes';
CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity());
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;
