-- Tags: no-fasttest, no-ordinary-database

-- Tests that vector similarity indexes can be detached/attached.

SET allow_experimental_vector_similarity_index = 1;

-- Due to a limitation of usearch, it is currently not possible to create vector similarity indexes on bf16 columns.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab(id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
