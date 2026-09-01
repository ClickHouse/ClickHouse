-- Tags: no-random-settings, no-random-merge-tree-settings
-- `grace_hash` used to ignore `max_bytes_before_external_join`, now it spills on it. The memory budget is
-- tight here, so random per-test settings could push the baseline over the cap.

SET max_memory_usage = '160Mi';
SET max_bytes_before_external_join = '16Mi';
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;
-- Keep the hard cap out of the way, it must not be the reason this query spills.
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

SELECT 'a threshold below what a hash table costs still completes';
-- Every freshly split bucket looks oversized under such a threshold. Splitting has to stop once it
-- no longer shrinks the table, or the bucket count runs into `grace_hash_join_max_buckets`.
SELECT count()
FROM (SELECT number AS k FROM numbers(200000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(200000)) AS t2
USING (k)
SETTINGS max_bytes_before_external_join = 1, grace_hash_join_initial_buckets = 1;
