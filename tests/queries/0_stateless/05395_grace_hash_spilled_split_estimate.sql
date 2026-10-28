-- The hash table of numeric keys is several times larger than their spilled blocks. Once the in-memory bucket
-- measures that ratio at its first split, spilled buckets split during input, and their children inherit an estimate
-- of the parent's historical rows. Duplicate keys in later blocks exercise inherited-file ordering for `ANY` joins.
SET join_algorithm = 'grace_hash';
SET max_bytes_before_external_join = '2Mi';
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
SET max_bytes_in_join = 0;
SET max_rows_in_join = 0;
SET max_threads = 1;
SET max_block_size = 4096;
SET join_use_nulls = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 'false';
SET legacy_join_size_limits_trigger_spilling = 0;
SET enable_analyzer = 1;

SELECT 'inner', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ALL INNER JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k);
SELECT 'full', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ALL FULL JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k);

SELECT 'any first', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ANY LEFT JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_any_take_last_row = 0;
SELECT 'any last', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ANY LEFT JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_any_take_last_row = 1;

-- Parallel input splits spilled buckets concurrently with writes to them.
SELECT 'parallel inner', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers_mt(200000)) AS l
ALL INNER JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers_mt(300000)) AS r USING (k)
SETTINGS max_threads = 4;
SELECT 'parallel full', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers_mt(200000)) AS l
ALL FULL JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers_mt(300000)) AS r USING (k)
SETTINGS max_threads = 4;

-- The same inputs establish the in-memory reference results.
SELECT 'hash inner', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ALL INNER JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;
SELECT 'hash full', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ALL FULL JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;
SELECT 'hash any first', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ANY LEFT JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, join_any_take_last_row = 0;
SELECT 'hash any last', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(200000)) AS l
ANY LEFT JOIN (SELECT number % 250000 AS k, number + 1 AS rv FROM numbers(300000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, join_any_take_last_row = 1;

-- Late calibration with several initial buckets: the spilled buckets stay below the threshold in raw bytes until the
-- in-memory bucket first splits. Then one large block of duplicate keys arrives, and `ANY` drops the duplicates
-- from the in-memory bucket, so it does not split again. The spilled buckets are estimated several times over the
-- threshold and split once during input, and the halves are planned again after the build.
CREATE TABLE late_right (k UInt64, rv UInt64) ENGINE = Memory;
INSERT INTO late_right SELECT number, number + 1 FROM numbers(80000)
SETTINGS max_block_size = 80000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO late_right SELECT number % 80000, 100000 + number + 1 FROM numbers(400000)
SETTINGS max_block_size = 400000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'late any first', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ANY LEFT JOIN late_right AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 8, join_any_take_last_row = 0;
SELECT 'late any last', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ANY LEFT JOIN late_right AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 8, join_any_take_last_row = 1;
SELECT 'late inner', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ALL INNER JOIN late_right AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 8;
SELECT 'late full', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ALL FULL JOIN late_right AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 8;

SELECT 'late hash any first', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ANY LEFT JOIN late_right AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, join_any_take_last_row = 0;
SELECT 'late hash any last', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ANY LEFT JOIN late_right AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, join_any_take_last_row = 1;
SELECT 'late hash inner', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ALL INNER JOIN late_right AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;
SELECT 'late hash full', count(), sum(lv), sum(rv)
FROM (SELECT number * 3 AS k, number + 1 AS lv FROM numbers(40000)) AS l
ALL FULL JOIN late_right AS r USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;

DROP TABLE late_right;
