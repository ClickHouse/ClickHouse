-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's parallel layout needs is unpredictable with sanitizers

-- A hash join's parallel layout costs about 1 MiB, so sixteen nested joins need around 16 MiB before a
-- row is read, and an 8 MiB limit has to stop them.

SET max_threads = 8;
SET join_algorithm = 'hash';

-- `parallel_hash_join_threshold = 0` keeps the parallel layout even when an earlier run left a small row estimate.
SET parallel_hash_join_threshold = 0;

SET max_memory_usage = '8Mi';
EXPLAIN
WITH
    c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
    c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
    c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
    c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
    c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
SELECT count() FROM c4; -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_memory_usage = '512Mi';
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
