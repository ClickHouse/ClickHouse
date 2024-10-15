-- Tags: no-fasttest, long, no-asan, no-asan, no-ubsan, no-debug
-- ^^ Disable test for slow builds: generating data takes time but a sufficiently large data set
-- is necessary for different hnsw_candidate_list_size_for_search settings to make a difference

-- Tests vector search with setting 'hnsw_candidate_list_size_for_search'

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 0;

DROP TABLE IF EXISTS tab;

-- Generate some data set that is large enough 
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab SELECT number, [toFloat32(randCanonical(1)), toFloat32(randCanonical(2))] FROM numbers(500000); -- if the test fails sporadically, increase the table size, HNSW is non-deterministic ...

DROP TABLE IF EXISTS results;
CREATE TABLE results(id Int32) ENGINE = Memory;

-- Standard vector search with default hnsw_candidate_list_size_for_search = 64
INSERT INTO results
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0.5, 0.5])
    LIMIT 1;

-- Vector search with custom hnsw_candidate_list_size_for_search
INSERT INTO results
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0.5, 0.5])
    LIMIT 1
    SETTINGS hnsw_candidate_list_size_for_search = 1;

-- Expect that matches are different
SELECT count(distinct *) FROM results;

DROP TABLE results;
DROP TABLE tab;
