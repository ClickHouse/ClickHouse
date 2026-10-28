-- Large string keys make spilled buckets split during input, before their historical files are read.
-- Duplicate keys in later blocks exercise inherited-file ordering for `ANY` joins.
SET join_algorithm = 'grace_hash';
SET max_bytes_before_external_join = 131072;
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 4;
SET grace_hash_join_max_buckets = 257;
SET max_bytes_in_join = 0;
SET max_rows_in_join = 0;
SET max_threads = 1;
SET max_block_size = 128;
SET join_use_nulls = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 'false';
SET legacy_join_size_limits_trigger_spilling = 0;
SET enable_analyzer = 1;

CREATE TABLE rebucket_left (k String, lv UInt64) ENGINE = Memory;
CREATE TABLE rebucket_right (k String, rv UInt64, payload String) ENGINE = Memory;

INSERT INTO rebucket_left
SELECT concat(toString(number), repeat('k', 96)), number + 1 FROM numbers(6000);
INSERT INTO rebucket_right
SELECT concat(toString(number % 5000), repeat('k', 96)), number + 1, repeat('v', 128) FROM numbers(20000);
INSERT INTO rebucket_right
SELECT concat(toString(number + 7000), repeat('k', 96)), number + 20001, repeat('v', 128) FROM numbers(1000);

SELECT 'inner', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL INNER JOIN rebucket_right USING (k);
SELECT 'left', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL LEFT JOIN rebucket_right USING (k);
SELECT 'right', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL RIGHT JOIN rebucket_right USING (k);
SELECT 'full', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL FULL JOIN rebucket_right USING (k);

SELECT 'any first', count(), sum(lv), sum(rv)
FROM rebucket_left ANY LEFT JOIN (SELECT * FROM rebucket_right ORDER BY rv) AS r USING (k)
SETTINGS join_any_take_last_row = 0;
SELECT 'any last', count(), sum(lv), sum(rv)
FROM rebucket_left ANY LEFT JOIN (SELECT * FROM rebucket_right ORDER BY rv) AS r USING (k)
SETTINGS join_any_take_last_row = 1;

SELECT 'semi', count(), sum(lv)
FROM rebucket_left LEFT SEMI JOIN rebucket_right USING (k);
SELECT 'anti', count(), sum(lv)
FROM rebucket_left LEFT ANTI JOIN rebucket_right USING (k);

SELECT 'nulls', count(), countIf(isNull(lv)), countIf(isNull(rv))
FROM rebucket_left ALL FULL JOIN rebucket_right USING (k)
SETTINGS join_use_nulls = 1;

-- Initial bucket 0 also splits repeatedly. Parallel input exercises stale routing selectors.
SELECT 'parallel inner', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL INNER JOIN rebucket_right USING (k)
SETTINGS max_threads = 4, grace_hash_join_initial_buckets = 1;
SELECT 'parallel full', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL FULL JOIN rebucket_right USING (k)
SETTINGS max_threads = 4, grace_hash_join_initial_buckets = 1, max_joined_block_size_rows = 17;

-- The same inputs establish the in-memory reference results.
SELECT 'hash inner', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL INNER JOIN rebucket_right USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;
SELECT 'hash full', count(), sum(lv), sum(rv), sum(length(payload))
FROM rebucket_left ALL FULL JOIN rebucket_right USING (k)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;

-- The two keys share an initial partition and separate on its next bit. One local split fits a three-leaf cap.
SELECT 'single local split', count(), sum(rv)
FROM (SELECT toUInt64(2) AS k UNION ALL SELECT toUInt64(3) AS k) AS l
ALL INNER JOIN (SELECT 2 + number % 2 AS k, number AS rv FROM numbers(20000)) AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 2, grace_hash_join_max_buckets = 3, max_block_size = 40000;

-- A single-key prefix must not split repeatedly before a second key arrives.
SELECT 'skewed', count(), sum(rv)
FROM (SELECT 2 + number AS k FROM numbers(2)) AS l
ALL INNER JOIN (SELECT if(number = 19999, toUInt64(3), toUInt64(2)) AS k, number AS rv FROM numbers(20000)) AS r USING (k)
SETTINGS grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 3;

DROP TABLE rebucket_left;
DROP TABLE rebucket_right;
