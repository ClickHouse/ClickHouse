-- A SHARED typed path inside a JSON member of Nested, read through the flattened Nested columns of a
-- Wide part. Depends on the fix for flattened Nested members beside a JSON member (PR 122519).

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (x UInt64, n Nested(j JSON(SHARED a UInt64, SHARED s String), k UInt8)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
INSERT INTO t_nested SELECT number, arrayMap(i -> CAST(concat('{"a":', toString(number * 10 + i), ',"s":"s', toString(i), '","d":', toString(i), '}'), 'JSON(SHARED a UInt64, SHARED s String)'), range(number % 3)), arrayMap(i -> toUInt8(i), range(number % 3)) FROM numbers(12);
SELECT x, n.j.a, n.j.s, n.k, n.j.d, n.j.size0 FROM t_nested ORDER BY x;
SELECT n.j FROM t_nested WHERE x = 5;
SELECT x, n.j.a, n.k FROM t_nested WHERE x >= 7 AND x < 10 ORDER BY x;
SELECT sum(length(n.j.a)), sum(arraySum(n.j.a)), sum(arraySum(n.k)) FROM t_nested;
SELECT x, n.j.a, n.k FROM t_nested ORDER BY x LIMIT 4 SETTINGS max_block_size = 5, max_threads = 1;
DROP TABLE t_nested;
