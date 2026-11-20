-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's parallel layout needs is unpredictable with sanitizers

SET max_threads = 256, join_algorithm = 'hash';

-- Keep the parallel layout even when an earlier run left a small row estimate.
SET hash_join_layout = 'parallel';

-- A bucket of the parallel layout gets its cells at the first insert of its slot, so planning the join fits in 1 MiB.
SET max_memory_usage = '1Mi';
SELECT count() > 0 FROM (
    EXPLAIN
    SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
    INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
);
