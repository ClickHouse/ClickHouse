-- `GROUP BY` without aggregate functions uses the void-mapped (set) hash tables, whose cells hold no
-- aggregate state, so the parallel single-level merge combines them by a plain key union. Every result
-- below must be identical with the setting on and off; the reference holds the exact values, and the
-- paired-subquery checks assert the equivalence directly.
-- The explicit thresholds pin the tables to the single-level layout (so the partitioned merge actually
-- runs regardless of randomized settings), and disabling the hash-table statistics keeps the layout
-- independent of previous runs.

SET enable_parallel_single_level_merge = 1;
SET max_threads = 4;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
-- The test pins the merge path; in-order aggregation would bypass it.
SET optimize_aggregation_in_order = 0;

SELECT 'key64_void';
SELECT count(), sum(g) FROM (SELECT intDiv(number, 15) AS g FROM numbers_mt(300000) GROUP BY g);

SELECT 'keys128_void';
SELECT count(), sum(a), sum(b) FROM (SELECT intDiv(number, 30) AS a, number % 7 AS b FROM numbers_mt(300000) GROUP BY a, b);

SELECT 'serialized_void';
SELECT count(), sum(a), sum(cityHash64(b)) FROM (
    SELECT intDiv(number, 100) AS a,
           concat(toString(number % 90), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', '')) AS b
    FROM numbers_mt(300000) GROUP BY a, b);

-- The NULL group of a single nullable key lives in a dedicated slot outside the cells and belongs to
-- partition 0. `toUInt64` keeps the key 64-bit wide: the narrower `nullable_key16` has no set variant.
SELECT 'nullable_key64_void with NULLs';
SELECT g FROM (SELECT nullIf(toUInt64(intDiv(number, 20) % 5000), 3) AS g FROM numbers_mt(300000) GROUP BY g) WHERE g IS NULL OR g < 2 ORDER BY g;

-- Several nullable keys pack their null map into the key itself, so there is no dedicated NULL slot.
SELECT 'nullable_keys256_void with NULLs';
SELECT count(), countIf(a IS NULL), countIf(b IS NULL) FROM (
    SELECT nullIf(toUInt64(intDiv(number, 40) % 3000), 5) AS a, nullIf(toUInt64(number % 11), 7) AS b
    FROM numbers_mt(300000) GROUP BY a, b);

SELECT 'empty result set';
SELECT number AS g FROM numbers_mt(300000) WHERE 0 GROUP BY g;
SELECT 'ok';

SELECT 'equivalence with the serial merge';
SELECT
    (SELECT sum(cityHash64(g)) FROM (SELECT intDiv(number, 15) AS g FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(g)) FROM (SELECT intDiv(number, 15) AS g FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 1),
    (SELECT sum(cityHash64(a, b)) FROM (SELECT intDiv(number, 100) AS a, toString(number % 90) AS b FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(a, b)) FROM (SELECT intDiv(number, 100) AS a, toString(number % 90) AS b FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_parallel_single_level_merge = 1),
    (SELECT sum(cityHash64(g)) FROM (SELECT nullIf(toUInt64(intDiv(number, 20) % 5000), 3) AS g FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(g)) FROM (SELECT nullIf(toUInt64(intDiv(number, 20) % 5000), 3) AS g FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 1);
