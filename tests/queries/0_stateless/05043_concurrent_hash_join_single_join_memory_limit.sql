-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- their allocator overhead is not what a memory limit this small should measure

SET max_threads = 256, join_algorithm = 'parallel_hash';

-- Above `parallel_hash_join_threshold` a hot run's row estimate keeps the parallel build; the limit is
-- about the plan-time cost of that build, whatever the estimate says.
SET parallel_hash_join_threshold = 0;

-- A join for 256 threads costs a few kilobytes at plan time - the lane and scratch slot tables, no
-- table and no lanes until rows arrive - so `EXPLAIN` fits in 512 KiB. The former parallel layout
-- allocated a bucket layout of about a megabyte per join in the constructor, under the four megabytes a
-- thread may leave uncharged, and only a flush at the end of the constructor made the same plan hit
-- this limit; the constructor keeps that flush so a plan is charged what it allocates as it goes.
SET max_memory_usage = '512Ki';
SELECT count() > 0 FROM (
    EXPLAIN
    SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
    INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
);

SET max_memory_usage = '512Mi';
SELECT count() > 0 FROM (
    EXPLAIN
    SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
    INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
);
