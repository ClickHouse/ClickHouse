-- The parallel single-level merge combines the per-thread aggregation hash tables by key-hash
-- partitions instead of serially. Every result below must be identical with the setting on and off; the
-- reference holds the exact values, and the paired-subquery checks assert the equivalence directly.
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

SELECT 'key64 sum';
SELECT count(), sum(c), sum(s) FROM (SELECT intDiv(number, 15) AS g, count() AS c, sum(number) AS s FROM numbers_mt(300000) GROUP BY g);

SELECT 'simple count';
SELECT count(), sum(c) FROM (SELECT intDiv(number, 15) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g);

SELECT 'string keys, short and long';
SELECT count(), sum(c), sum(s) FROM (
    SELECT concat('k', toString(intDiv(number, 20)), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', '')) AS g,
           count() AS c, sum(number) AS s
    FROM numbers_mt(300000) GROUP BY g);

SELECT 'empty string key in its dedicated slot';
SELECT g = '', c FROM (SELECT if(number % 7 = 0, '', toString(intDiv(number, 25))) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g) ORDER BY c DESC LIMIT 1;

SELECT 'serialized two keys';
SELECT count(), sum(c) FROM (SELECT intDiv(number, 100) AS a, toString(number % 90) AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b);

SELECT 'keys128';
SELECT count(), sum(c) FROM (SELECT intDiv(number, 30) AS a, number % 7 AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b);

SELECT 'nullable key with NULLs';
SELECT g, c FROM (SELECT nullIf(intDiv(number, 20) % 5000, 3) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g) WHERE g IS NULL OR g < 2 ORDER BY g;

SELECT 'low cardinality string key';
SELECT count(), sum(c) FROM (SELECT toLowCardinality(toString(number % 5000)) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g);

SELECT 'uniqExact heavy states';
SELECT count(), sum(u) FROM (SELECT intDiv(number, 40) AS g, uniqExact(number % 997) AS u FROM numbers_mt(300000) GROUP BY g);

SELECT 'with totals';
SELECT intDiv(number, 50) AS g, count() FROM numbers_mt(300000) GROUP BY g WITH TOTALS ORDER BY g LIMIT 3;

SELECT 'early limit';
SELECT intDiv(number, 15) AS g, count() FROM numbers_mt(3000000) GROUP BY g LIMIT 1 FORMAT Null;
SELECT 'ok';

SELECT 'two threads';
SELECT count(), sum(c) FROM (SELECT intDiv(number, 15) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g) SETTINGS max_threads = 2;

SELECT 'tables under the partition threshold take the serial fallback';
SELECT count(), sum(c), sum(s) FROM (SELECT intDiv(number, 3000) AS g, count() AS c, sum(number) AS s FROM numbers_mt(300000) GROUP BY g);

SELECT 'empty result set';
SELECT number AS g, count() FROM numbers_mt(300000) WHERE 0 GROUP BY g;
SELECT 'ok';

SELECT 'equivalence with the serial merge';
SELECT
    (SELECT sum(cityHash64(g, c, s)) FROM (SELECT intDiv(number, 15) AS g, count() AS c, sum(number) AS s FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(g, c, s)) FROM (SELECT intDiv(number, 15) AS g, count() AS c, sum(number) AS s FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 1),
    (SELECT sum(cityHash64(g, c)) FROM (SELECT concat('k', toString(intDiv(number, 20)), if(number % 3 = 0, '_long', '')) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(g, c)) FROM (SELECT concat('k', toString(intDiv(number, 20)), if(number % 3 = 0, '_long', '')) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 1),
    (SELECT sum(cityHash64(g, u)) FROM (SELECT intDiv(number, 40) AS g, uniqExact(number % 997) AS u FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(g, u)) FROM (SELECT intDiv(number, 40) AS g, uniqExact(number % 997) AS u FROM numbers_mt(300000) GROUP BY g) SETTINGS enable_parallel_single_level_merge = 1);
