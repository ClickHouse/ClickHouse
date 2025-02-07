-- Tags: no-fasttest, no-ordinary-database

-- Tests that no vector similarity indexes can be created on Array(BFloat16) columns.
-- (This is because of a limitation in usearch which does not allow ingest of bf16)
-- --> https://github.com/unum-cloud/usearch/issues/553

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
