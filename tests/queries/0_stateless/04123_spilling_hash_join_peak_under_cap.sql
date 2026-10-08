-- Regression test: with `max_bytes_before_external_join` set, the in-memory peak of
-- a spilling hash join must stay below `max_memory_usage`. Previously several issues
-- in `ConcurrentHashJoin`, `SpillingHashJoin` and `GraceHashJoin` could conspire to
-- make the in-memory hash table grow well past the configured external-join
-- threshold, causing a `MEMORY_LIMIT_EXCEEDED` exception during the build phase.

-- The right side has 3M rows of an integer key. The build-side hash table grows
-- well past `max_bytes_before_external_join = 64 MiB`, and on a power-of-two doubling
-- transiently allocates a half-gigabyte buffer that blows past `max_memory_usage = 256 MiB`.
-- With the fixes in place, `SpillingHashJoin` switches to `GraceHashJoin` before that
-- doubling can happen and the query stays under the cap.

SET max_memory_usage = '256Mi';
SET max_bytes_before_external_join = '64Mi';
SET grace_hash_join_initial_buckets = 1;

SELECT 'single-thread hash';
SET join_algorithm = 'hash';
SET max_threads = 1;
SELECT count()
FROM (SELECT number AS k FROM numbers(3000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(3000000)) AS t2
USING (k);

SELECT 'concurrent parallel_hash';
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;
SELECT count()
FROM (SELECT number AS k FROM numbers(3000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(3000000)) AS t2
USING (k);
