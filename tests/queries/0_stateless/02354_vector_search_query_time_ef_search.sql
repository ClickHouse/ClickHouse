-- Tags: no-fasttest

-- Tests vector search with setting 'ef_search'

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 0;

DROP TABLE IF EXISTS tab;

-- Generate some data set that is large enough 
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab SELECT number, [toFloat32(randCanonical(1)), toFloat32(randCanonical(2))] FROM numbers(500000); -- if the test fails sporadically, increase the table size, HNSW is non-deterministic ...

-- Value = 0 is illegal.
WITH [0.5, 0.5] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS ef_search = 0; -- { serverError INCORRECT_DATA }

DROP TABLE IF EXISTS results;
CREATE TABLE results(id Int32) ENGINE = Memory;

-- Standard vector search, ef_search is by default 64
INSERT INTO results
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0.5, 0.5])
    LIMIT 1;

-- Vector search with custom ef_search
INSERT INTO results
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0.5, 0.5])
    LIMIT 1
    SETTINGS ef_search = 1;

-- Expect that matches are different
SELECT count(distinct *) from results;

DROP TABLE results;
DROP TABLE tab;
