-- `grace_hash` listed alone needs a spill threshold to run. INTERSECT DISTINCT and EXCEPT DISTINCT are only
-- executed as a join when it has one, and keep the set-operation step otherwise.

SET enable_analyzer = 1;
SET join_algorithm = 'grace_hash';
SET legacy_join_size_limits_trigger_spilling = 0;

SELECT 'without a spill threshold';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SELECT * FROM (SELECT number AS x FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) ORDER BY x;
SELECT * FROM (SELECT number AS x FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY x;
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
WHERE explain LIKE '%Join%';
SELECT count() > 0 FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
WHERE explain LIKE '%IntersectOrExcept%';

SELECT 'with a spill threshold';
SET max_bytes_before_external_join = '1G';
SELECT * FROM (SELECT number AS x FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) ORDER BY x;
SELECT * FROM (SELECT number AS x FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY x;
SELECT count() > 0 FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
WHERE explain LIKE '%Join%';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
WHERE explain LIKE '%IntersectOrExcept%';
