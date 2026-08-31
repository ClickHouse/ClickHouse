-- Tags: no-random-settings, no-random-merge-tree-settings
-- `grace_hash` now spills on `max_bytes_before_external_join` instead of ignoring it. The memory budget
-- is tight, so random per-test settings could push the baseline over the cap.

SET max_memory_usage = '160Mi';
SET max_bytes_before_external_join = '16Mi';
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;
-- The documented hard cap stays out of the way: it must not be what makes this query spill.
SET max_bytes_in_join = 0;
SET max_rows_in_join = 0;
SET join_algorithm = 'grace_hash';

SELECT 'single thread';
SET max_threads = 1;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'many threads';
SET max_threads = 4;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'explain still reports grace hash';
SELECT countIf(explain LIKE '%Algorithm: GraceHashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM (SELECT number AS k FROM numbers(10)) AS t1
    INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2 USING (k)
);
