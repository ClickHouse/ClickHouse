-- Tags: long

-- This test was split in two due to long runtimes in sanitizers.
-- The other part is 00284_external_aggregation_2.

SET max_bytes_before_external_group_by = 100000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 410000000;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET max_execution_time = 300;

SELECT sum(k), sum(c) FROM (SELECT number AS k, count() AS c FROM (SELECT * FROM system.numbers LIMIT 10000000) GROUP BY k);
SELECT sum(k), sum(c), max(u) FROM (SELECT number AS k, count() AS c, uniqArray(range(number % 16)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k);
