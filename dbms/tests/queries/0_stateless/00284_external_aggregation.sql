SET max_bytes_before_external_group_by = 100000000;
SET max_memory_usage = 201000000;

SELECT sum(k), sum(c) FROM (SELECT number AS k, count() AS c FROM (SELECT * FROM system.numbers LIMIT 10000000) GROUP BY k);
SELECT sum(k), sum(c), max(u) FROM (SELECT number AS k, count() AS c, uniqArray(range(number % 16)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k);
