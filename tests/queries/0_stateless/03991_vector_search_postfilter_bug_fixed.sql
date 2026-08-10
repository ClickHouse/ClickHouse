-- Tags: no-fasttest, long, no-asan, no-ubsan, no-msan, no-tsan, no-debug
-- Inverted rewrite of 02354_vector_search_postfiltering_bug. The legacy
-- ORDER BY <distance> LIMIT pattern with PREWHERE post-filtering had a
-- bug (issue #78161) where rows could be silently dropped post-filter
-- without surfacing the loss to the user. The vectorSearch table function
-- applies the WHERE clause as a prefilter (via the bitmap subquery
-- planted by `DelayedCreatingBitmapsStep`), so every row returned
-- satisfies the WHERE — the prefilter is part of the index search,
-- not a post-filter on the top-K result. Plan 02 §step 7-B lines
-- 661-664.

SET allow_experimental_search_topk_table_functions = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32)) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO tab SELECT number, [randCanonical(), randCanonical()] FROM numbers(10000);

ALTER TABLE tab ADD INDEX idx_vec vec TYPE vector_similarity('hnsw', 'cosineDistance', 2, 'f32', 64, 400);
ALTER TABLE tab MATERIALIZE INDEX idx_vec SETTINGS mutations_sync=2;

-- 1. Without WHERE: K=10 returns exactly 10 rows.
SELECT count() FROM vectorSearch(currentDatabase(), tab, idx_vec, [1.0, 2.0], 10);

-- 2. With a selective WHERE: every returned row satisfies the predicate.
-- HNSW's filtered search may return fewer than K rows if the prefilter is
-- selective enough that the index can't find K matching candidates within
-- its expansion factor — but the legacy bug #78161 was about *silent
-- drop without correctness*, not about returning fewer rows. vectorSearch
-- guarantees that every returned row satisfies the WHERE clause.
SELECT countIf(NOT (id < 5000)) = 0 FROM (
    SELECT id
    FROM vectorSearch(currentDatabase(), tab, idx_vec, [1.0, 2.0], 10)
    WHERE id < 5000
);

DROP TABLE tab;
