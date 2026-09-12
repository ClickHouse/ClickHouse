-- A `GROUP BY` over several long string keys and without aggregate functions is served by the
-- serialized method with key-only cells, which writes the whole block's keys into the arena at once
-- and then compacts the duplicate rows' bytes away. Check its results against the mapped path,
-- which is not touched by that, under the settings that change how blocks reach it.

SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT leftPad(toString(number % 977), 100, '0') AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2);

SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2, count() FROM (SELECT leftPad(toString(number % 977), 100, '0') AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2);

SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT leftPad(toString(number % 977), 100, '0') AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2)
SETTINGS max_block_size = 1000;

SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT leftPad(toString(number % 977), 100, '0') AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2)
SETTINGS group_by_two_level_threshold = 1000, group_by_two_level_threshold_bytes = 100000;

SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT leftPad(toString(number % 977), 100, '0') AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers_mt(100000)) GROUP BY k1, k2)
SETTINGS max_threads = 8, group_by_two_level_threshold = 1000;

-- Nullable keys go through the same path with a null map alongside.
SELECT count() AS distinct_rows, sum(cityHash64(ifNull(k1, ''), k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT if(number % 11 = 0, NULL, leftPad(toString(number % 977), 100, '0')) AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2);

SELECT count() AS distinct_rows, sum(cityHash64(ifNull(k1, ''), k2)) AS keys_hash
FROM (SELECT k1, k2, count() FROM (SELECT if(number % 11 = 0, NULL, leftPad(toString(number % 977), 100, '0')) AS k1, leftPad(toString(number % 13), 100, '0') AS k2 FROM numbers(100000)) GROUP BY k1, k2);

-- A block whose rows are all the same constant key.
SELECT count() AS distinct_rows, sum(cityHash64(k1, k2)) AS keys_hash
FROM (SELECT k1, k2 FROM (SELECT materialize(repeat('a', 200)) AS k1, materialize(repeat('b', 200)) AS k2 FROM numbers(50000)) GROUP BY k1, k2);
