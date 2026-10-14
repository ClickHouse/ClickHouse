-- The right side of this `hash` join is larger than `max_bytes_before_external_join`, so the join
-- spills to disk and continues as a grace hash join. Several threads fill its in-memory part. The
-- join must finish without a `LOGICAL_ERROR` exception and find all 2000000 matches.

SET max_threads = 8;
SET max_block_size = 8192;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 16777216;
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;

SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);
