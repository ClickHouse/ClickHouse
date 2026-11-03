-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's parallel layout needs is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'hash';

-- Keep the parallel layout even when an earlier run left a small row estimate.
SET hash_join_layout = 'parallel';

-- The layout (about 1 MiB) is below the 4 MiB a thread may leave uncharged, yet it must count against
-- `max_memory_usage`, so `max_untracked_memory` stays at its default.
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
