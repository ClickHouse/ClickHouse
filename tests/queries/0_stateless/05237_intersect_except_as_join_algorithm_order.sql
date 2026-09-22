-- The join algorithms are tried in the order they are listed in, so INTERSECT DISTINCT and EXCEPT DISTINCT are
-- executed as a join only when the first algorithm of the list that can execute it does not fail it first.

SET enable_analyzer = 1;
SET legacy_join_size_limits_trigger_spilling = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT 'an algorithm that cannot execute the join is passed over';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM (SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) SETTINGS join_algorithm = 'grace_hash,hash')
WHERE explain LIKE '%Join%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM (SELECT number FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) SETTINGS join_algorithm = 'partial_merge,hash')
WHERE explain LIKE '%Join%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM (SELECT number FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) SETTINGS join_algorithm = 'direct,full_sorting_merge,parallel_hash')
WHERE explain LIKE '%Join%';
SELECT * FROM (SELECT number AS x FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'grace_hash,hash';
SELECT * FROM (SELECT number AS x FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'partial_merge,hash';

SELECT 'an algorithm that fails the join keeps the set-operation step';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM (SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) SETTINGS join_algorithm = 'grace_hash')
WHERE explain LIKE '%IntersectOrExcept%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM (SELECT number FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) SETTINGS join_algorithm = 'full_sorting_merge,partial_merge')
WHERE explain LIKE '%IntersectOrExcept%';
SELECT * FROM (SELECT number AS x FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'grace_hash';
SELECT * FROM (SELECT number AS x FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'full_sorting_merge,partial_merge';
