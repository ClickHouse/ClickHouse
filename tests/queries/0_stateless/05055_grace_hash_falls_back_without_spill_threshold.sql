-- `join_algorithm` is a preference list: with no spill threshold `grace_hash` cannot run, so the next
-- algorithm in the list takes over instead of the query failing.

SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

SELECT 'grace_hash,hash falls back to hash';
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2 USING (k)
SETTINGS join_algorithm = 'grace_hash,hash';

SELECT 'grace_hash alone is still rejected';
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2 USING (k)
SETTINGS join_algorithm = 'grace_hash'; -- { serverError BAD_ARGUMENTS }

SELECT 'legacy mode keeps grace_hash usable on its own';
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2 USING (k)
SETTINGS join_algorithm = 'grace_hash', legacy_join_size_limits_trigger_spilling = 1;
