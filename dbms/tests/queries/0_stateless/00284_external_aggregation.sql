SET max_bytes_before_external_group_by = 100000000;
-- In worst case, 256 two-level hash tables could be resized at the same time.
-- Also, small hash tables have factor 4.
SET max_memory_usage = 301000000;

set max_block_size=8192;
SELECT sum(k), sum(c) FROM (SELECT number AS k, count() AS c FROM (SELECT * FROM system.numbers LIMIT 10000000) GROUP BY k) settings max_block_size=8192;
SELECT sum(k), sum(c), max(u) FROM (SELECT number AS k, count() AS c, uniqArray(range(number % 16)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k) settings max_block_size=8192;
