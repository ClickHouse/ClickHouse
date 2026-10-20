-- The deferred build stores the right blocks first and inserts their keys once it knows how many distinct keys there
-- are. Every query must give the same result as the streaming build, with both layouts: `ALL` joins over duplicate-heavy
-- and unique keys, and the non-joined rows of `RIGHT` and `FULL` joins.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();

-- Every 16th key has 17 rows, every other 4th key 3 rows, the rest one.
INSERT INTO t_build
SELECT k, concat('key_', toString(k)), k * 7
FROM
(
    SELECT number AS k, arrayJoin(range(multiIf(number % 16 = 0, 17, number % 4 = 0, 3, 1))) AS copy
    FROM numbers(100000)
);

-- Two rows per key, and half of the keys miss the right side.
INSERT INTO t_probe
SELECT k, concat('key_', toString(k)), k * 13
FROM (SELECT 50000 + intDiv(number, 2) AS k FROM numbers(200000));


SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '04327_off';
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '04327_on';
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '04327_on';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, count(), countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1), countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND log_comment IN ('04327_off', '04327_on')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_build;
DROP TABLE t_probe;
