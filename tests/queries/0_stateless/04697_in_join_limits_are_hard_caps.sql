-- `max_rows_in_join` / `max_bytes_in_join` are hard caps for spilling joins too: they used to be
-- silently downgraded to a spill trigger once a join went external.

SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_join = '1M';
SET grace_hash_join_initial_buckets = 1;

SELECT 'max_bytes_in_join stops a spilling join';
SET max_bytes_in_join = '4M';
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k); -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'max_rows_in_join stops a spilling join';
SET max_bytes_in_join = 0;
SET max_rows_in_join = 1000;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k); -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'join_overflow_mode = break truncates instead of throwing';
SET join_overflow_mode = 'break';
SELECT count() > 0
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'no cap: spilling completes the join';
SET join_overflow_mode = 'throw';
SET max_rows_in_join = 0;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'grace_hash without any spill threshold is rejected';
SET max_bytes_before_external_join = 0;
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2
USING (k)
SETTINGS join_algorithm = 'grace_hash'; -- { serverError BAD_ARGUMENTS }

SELECT 'grace_hash with a spill threshold works';
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2
USING (k)
SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = '1M';
