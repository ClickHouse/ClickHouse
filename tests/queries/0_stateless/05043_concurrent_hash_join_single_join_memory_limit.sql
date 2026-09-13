-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's bucket layout needs is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'parallel_hash';

-- Once a join has run here, the hash table statistics can give a later run a row estimate, and an
-- estimate below `parallel_hash_join_threshold` picks the serial layout, which costs nothing. Pin
-- the threshold so this test measures the bucketed layout however often it has run before.
SET parallel_hash_join_threshold = 0;

-- One join's bucket layout is about a megabyte, which is under the four megabytes a thread may
-- leave uncharged. Catching this needs the flush at the end of the constructor, not any single
-- allocation being refused, so `max_untracked_memory` is deliberately left at its default.
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
