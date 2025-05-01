-- Tags: no-fasttest, no-ordinary-database

-- Tests that various conditions are checked during creation of vector search indexes.

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Two or six index arguments';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity()) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('cant_have_one_arg')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('cant', 'have', 'three_args')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('cant', 'have', 'more', 'than', 'six', 'args', '!')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '1st argument (method) must be String and hnsw';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity(3, 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('not_hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

SELECT '2nd argument (distance function) must be String and L2Distance or cosineDistance';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'invalid_distance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

SELECT '3nd argument (quantization), if given, must be String and f32, f16, ...';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 1, 1, 1, 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'invalid', 2, 1, 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }
SELECT '4nd argument (M), if given, must be UInt64 and > 1';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 'invalid', 1, 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 1, 1, 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }
SELECT '5nd argument (ef_construction), if given, must be UInt64 and > 0';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 'invalid', 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 0, 1)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }
SELECT '6nd argument (ef_search), if given, must be UInt64 and > 0';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 1, 'invalid')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 2, 1, 0)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

SELECT 'Must be created on single column';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx (vec, id) TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on Array(Float32) columns';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tab(id Int32, vec UInt64, INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Float32, INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Array(Float64), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec LowCardinality(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Nullable(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

SELECT 'Rejects INSERTs of Arrays with different sizes';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;
