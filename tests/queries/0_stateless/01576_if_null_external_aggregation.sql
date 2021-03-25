SET max_bytes_before_external_group_by = 200000000;

SET max_memory_usage = 1500000000;
SET max_threads = 12;

SELECT bitAnd(number, pow(2, 20) - 1) as k, argMaxIf(k, number % 2 = 0 ? number : Null, number > 42),  uniq(number) AS u FROM numbers(1000000) GROUP BY k format Null;

