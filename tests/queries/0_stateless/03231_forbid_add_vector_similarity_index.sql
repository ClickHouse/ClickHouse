DROP TABLE IF EXISTS test_embedding;

CREATE TABLE test_embedding
(
	id UInt32,
    embedding Array(Float32),
)
ENGINE = MergeTree
ORDER BY tuple();

SET allow_experimental_vector_similarity_index = 0;

alter table test_embedding add INDEX idx embedding TYPE vector_similarity('hnsw', 'cosineDistance');  -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_vector_similarity_index = 1;

alter table test_embedding add INDEX idx embedding TYPE vector_similarity('hnsw', 'cosineDistance');

DROP TABLE test_embedding;
