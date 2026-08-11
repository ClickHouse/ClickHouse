-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite with a row policy.
--
-- A row policy restricts rows inside the reader, just like a `WHERE` or a `PREWHERE`, so it must count as an
-- additional filter. It did not, so a query filtered only by a policy was treated as unfiltered: an explicit
-- request for exact search via `vector_search_filter_strategy = 'prefilter'` was ignored, and the index
-- fetched only `LIMIT` neighbours without the `vector_search_index_fetch_multiplier` compensation, which the
-- policy can then discard.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP ROW POLICY IF EXISTS rp_04814 ON t_04814;
DROP TABLE IF EXISTS t_04814;

CREATE TABLE t_04814 (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04814 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

SELECT 'no policy, prefilter: uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

CREATE ROW POLICY rp_04814 ON t_04814 FOR SELECT USING id != 0 TO ALL;

SELECT 'policy, prefilter: exact search';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

SELECT 'policy, postfilter: uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'postfilter')
WHERE explain LIKE '%vector_similarity%';

DROP ROW POLICY rp_04814 ON t_04814;

-- A row policy that reads the vector column itself. The non-rescoring rewrite removes the physical vector
-- column from the read list, but the policy filter runs inside the reader and needs it, so the rewrite must
-- be skipped for this case (same treatment as a `SELECT` clause containing the vector column).
CREATE ROW POLICY rp_04814 ON t_04814 FOR SELECT USING length(vec) > 0 TO ALL;

SELECT 'policy on the vector column, no rescoring: correct result';
SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'policy on the vector column, rescoring: correct result';
SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

DROP ROW POLICY rp_04814 ON t_04814;
DROP TABLE t_04814;
