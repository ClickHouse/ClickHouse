-- Tags: no-fasttest, no-ordinary-database

-- getSecondaryIndexGranularity compared the index type name case-sensitively, so an uppercase
-- `VECTOR_SIMILARITY` skipped the default-granularity branch that lowercase `vector_similarity` takes.
-- With the case-insensitive match, an uppercase type gets the same default when granularity is omitted,
-- while an explicit GRANULARITY is still honored (the explicit-granularity return precedes the vector
-- branch, same as for the lowercase spelling).

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab_omitted;
DROP TABLE IF EXISTS tab_explicit;

CREATE TABLE tab_omitted (id UInt64, vec Array(Float32), INDEX idx vec TYPE VECTOR_SIMILARITY('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_explicit (id UInt64, vec Array(Float32), INDEX idx vec TYPE VECTOR_SIMILARITY('hnsw', 'L2Distance', 2) GRANULARITY 7)
ENGINE = MergeTree ORDER BY id;

-- Omitted granularity gets the vector default; an explicit GRANULARITY is honored.
SELECT table, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table IN ('tab_omitted', 'tab_explicit') ORDER BY table;

DROP TABLE tab_omitted;
DROP TABLE tab_explicit;
