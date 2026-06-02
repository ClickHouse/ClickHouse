SET max_bytes_before_external_join = 0; -- Auto spilling hash join has to be disabled to test switching to merge join.
SET max_bytes_in_join = '100', join_algorithm = 'auto';

SELECT 3 == count() FROM (SELECT toLowCardinality(toNullable(number)) AS l FROM system.numbers LIMIT 3) AS s1
ANY LEFT JOIN (SELECT toLowCardinality(toNullable(number)) AS r FROM system.numbers LIMIT 4) AS s2 ON l = r
;
