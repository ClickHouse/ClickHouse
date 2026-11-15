-- `join_overflow_mode = 'break'` in a `grace_hash` join: when the bucket being built crosses `max_rows_in_join`,
-- reading the right side stops. The buckets already spilled to disk must not be joined afterwards either,
-- or the join returns rows well past the cap. Here bucket 0 holds about half of the rows and trips the
-- cap while it is built; bucket 1 waits on disk and has to be dropped.

SET join_algorithm = 'grace_hash';
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_join = '16M';
SET grace_hash_join_initial_buckets = 2;
SET max_rows_in_join = 40000;
SET join_overflow_mode = 'break';

SELECT count() BETWEEN 40000 AND 99999
FROM (SELECT number AS k FROM numbers(100000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(100000)) AS t2
USING (k);

-- With `throw` the same query fails instead of truncating.
SET join_overflow_mode = 'throw';
SELECT count()
FROM (SELECT number AS k FROM numbers(100000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(100000)) AS t2
USING (k); -- { serverError SET_SIZE_LIMIT_EXCEEDED }
