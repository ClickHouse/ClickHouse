-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- memory consumption of building `parallel_hash` join hash tables is unpredictable with sanitizers

SET max_threads = 8;

SET max_memory_usage = '16Mi', join_algorithm = 'parallel_hash';
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

SET max_memory_usage = '16Mi', join_algorithm = 'hash';
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
