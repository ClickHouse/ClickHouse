-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- memory consumption of building `parallel_hash` join hash tables is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'parallel_hash';

SET max_memory_usage = '16Mi';
EXPLAIN
SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id); -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_memory_usage = '512Mi';
SELECT count() > 0 FROM (
    EXPLAIN
    SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
    INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
);
