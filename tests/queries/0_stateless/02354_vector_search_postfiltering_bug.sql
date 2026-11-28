-- Tags: no-fasttest, long, no-asan, no-ubsan, no-msan, no-tsan, no-debug
-- Test for Bug 78161

SET enable_analyzer = 1;

CREATE TABLE tab (id Int32, vec Array(Float32)) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO tab SELECT number, [randCanonical(), randCanonical()] FROM numbers(10000);

-- Create index
ALTER TABLE tab ADD INDEX idx_vec vec TYPE vector_similarity('hnsw', 'cosineDistance', 2, 'f32', 64, 400);
ALTER TABLE tab MATERIALIZE INDEX idx_vec SETTINGS mutations_sync=2;

WITH [1., 2.] AS reference_vec
SELECT *
FROM tab
PREWHERE id < 5000
ORDER BY cosineDistance(vec, reference_vec) ASC
LIMIT 10
FORMAT Null;
