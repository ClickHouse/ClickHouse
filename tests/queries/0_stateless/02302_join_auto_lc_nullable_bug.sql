-- Tags: no-backward-compatibility-check

SET max_bytes_in_join = '100', join_algorithm = 'auto';

SELECT 3 == count() FROM (SELECT toLowCardinality(toNullable(number)) AS l FROM system.numbers LIMIT 3) AS s1
ANY LEFT JOIN (SELECT toLowCardinality(toNullable(number)) AS r FROM system.numbers LIMIT 4) AS s2 ON l = r
;
