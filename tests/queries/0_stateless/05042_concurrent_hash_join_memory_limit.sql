-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's bucket layout needs is unpredictable with sanitizers

-- A join whose right side has no row-count estimate builds the bucketed layout. That layout costs
-- about a megabyte, so sixteen nested joins need around 16 MiB before a single row is read.
-- An 8 MiB limit has to stop them. `hash` and `parallel_hash` are one algorithm now, so both
-- names have to hit that limit.

SET max_threads = 8;

-- Once a join has run here, the hash table statistics can give a later run a row estimate, and an
-- estimate below `parallel_hash_join_threshold` picks the serial layout, which costs nothing.
SET parallel_hash_join_threshold = 0;

SET max_memory_usage = '8Mi', join_algorithm = 'parallel_hash';
EXPLAIN
WITH
    c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
    c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
    c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
    c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
    c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
SELECT count() FROM c4; -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_memory_usage = '8Mi', join_algorithm = 'hash';
EXPLAIN
WITH
    c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
    c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
    c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
    c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
    c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
SELECT count() FROM c4; -- { serverError MEMORY_LIMIT_EXCEEDED }

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
