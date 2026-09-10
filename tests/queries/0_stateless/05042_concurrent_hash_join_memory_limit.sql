-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- the memory a hash join's bucket layout needs is unpredictable with sanitizers

-- A join whose right side has no row-count estimate builds the bucketed layout, and one bucket's
-- map, arena and lock are each too small for the memory tracker to refuse on its own. Sixteen
-- nested joins can only be stopped by the running total, not by any single allocation.
-- `hash` and `parallel_hash` are one algorithm, so the name does not change what the join costs.

SET max_threads = 8;

SET max_memory_usage = '8Mi', join_algorithm = 'parallel_hash';
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

SET max_memory_usage = '8Mi', join_algorithm = 'hash';
EXPLAIN
WITH
    c0 AS (SELECT number AS id, number AS val FROM numbers(1)),
    c1 AS (SELECT a.id AS id, a.val + b.val AS val FROM c0 AS a INNER JOIN c0 AS b USING (id)),
    c2 AS (SELECT a.id AS id, a.val + b.val AS val FROM c1 AS a INNER JOIN c1 AS b USING (id)),
    c3 AS (SELECT a.id AS id, a.val + b.val AS val FROM c2 AS a INNER JOIN c2 AS b USING (id)),
    c4 AS (SELECT a.id AS id, a.val + b.val AS val FROM c3 AS a INNER JOIN c3 AS b USING (id))
SELECT count() FROM c4; -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_memory_usage = '512Mi', join_algorithm = 'hash';
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
