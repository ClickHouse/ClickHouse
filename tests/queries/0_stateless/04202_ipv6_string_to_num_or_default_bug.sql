-- Verify that IPv6StringToNumOrDefault properly zeroes all 16 bytes on failure.
-- Bug: only one byte was zeroed (vec_res[i] = 0 instead of filling all 16 bytes),
-- causing stale parse data to leak into "default" results.
-- https://github.com/ClickHouse/ClickHouse/pull/93543

SELECT IPv6NumToString(IPv6StringToNumOrDefault(if(number % 2 = 0, '::ffff:104.30.2.197', 'invalid'))) AS ip, count() AS c FROM numbers(100) GROUP BY ip ORDER BY ip;
