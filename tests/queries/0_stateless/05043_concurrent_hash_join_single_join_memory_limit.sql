-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's bucket layout needs is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'parallel_hash';

-- One join's bucket layout is about a megabyte, which fits inside the default
-- `max_untracked_memory`, so the thread-local counter has to be flushed on every allocation for
-- the query's tracker to see the overshoot at all.
SET max_untracked_memory = 1;

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
