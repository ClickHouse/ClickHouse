-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- their allocator overhead is not what a memory limit this small should measure

-- A hash join costs next to nothing at plan time: its table is created when the build side is known,
-- not when the join is constructed, so planning sixteen nested joins allocates a few kilobytes per join
-- and an 8 MiB limit lets `EXPLAIN` through. The former parallel layout allocated its 256 buckets per
-- join up front, about a megabyte each, and the same plan had to hit this limit; `hash` and
-- `parallel_hash` are one algorithm and behave alike.

SET max_threads = 8;

-- Above `parallel_hash_join_threshold` a hot run's row estimate keeps the parallel build; the limit is
-- about the plan-time cost of that build, whatever the estimate says.
SET parallel_hash_join_threshold = 0;

SET max_memory_usage = '8Mi', join_algorithm = 'parallel_hash';
SELECT count() > 0 FROM (
    EXPLAIN
    WITH
        c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
        c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
        c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
        c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
        c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
    SELECT count() FROM c4
);

SET max_memory_usage = '8Mi', join_algorithm = 'hash';
SELECT count() > 0 FROM (
    EXPLAIN
    WITH
        c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
        c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
        c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
        c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
        c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
    SELECT count() FROM c4
);

SET max_memory_usage = '512Mi', join_algorithm = 'parallel_hash';
SELECT count() > 0 FROM (
    EXPLAIN
    WITH
        c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
        c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
        c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
        c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
        c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
    SELECT count() FROM c4
);
