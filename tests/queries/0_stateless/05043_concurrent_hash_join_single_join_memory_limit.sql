-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's parallel layout needs is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'hash';

-- Once a join has run here, the hash table statistics can give a later run a row estimate, and an
-- estimate below `parallel_hash_join_threshold` picks the serial layout, which costs nothing. Pin
-- the threshold so this test measures the parallel layout however often it has run before.
SET parallel_hash_join_threshold = 0;

-- One join's parallel layout takes about a megabyte, less than the four megabytes a thread may
-- allocate without charging the query. The layout must still count against `max_memory_usage`,
-- so `max_untracked_memory` stays at its default.
SET max_memory_usage = '512Ki';
EXPLAIN
SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id); -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_memory_usage = '512Mi';
SELECT count() > 0 FROM (
    EXPLAIN
    SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
    INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
);
