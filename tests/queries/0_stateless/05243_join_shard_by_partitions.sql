DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_right_month;
DROP TABLE IF EXISTS t_right_datetime;
DROP TABLE IF EXISTS t_empty;

CREATE TABLE t_left (d Date, v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY v;
CREATE TABLE t_right (d Date, w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;
CREATE TABLE t_right_month (d Date, w UInt64) ENGINE = MergeTree PARTITION BY toMonth(d) ORDER BY w;
CREATE TABLE t_right_datetime (d DateTime('UTC'), w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;
CREATE TABLE t_empty (d Date, w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;

-- The left table has 2026-01 .. 2026-08, the right table has 2026-03 .. 2026-12.
INSERT INTO t_left SELECT toDate('2026-01-01') + intDiv(number, 3), number FROM numbers(3 * 243);
INSERT INTO t_right SELECT toDate('2026-03-01') + intDiv(number, 2), number FROM numbers(2 * 306);
INSERT INTO t_right_month SELECT * FROM t_right;
INSERT INTO t_right_datetime SELECT d, w FROM t_right;

-- `SpillingHashJoin` (chosen when spilling is enabled) is not sharded.
SET max_threads = 4, join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_shard_by_partitions = 1;

SELECT 'explain';
SELECT countIf(explain LIKE '%Sharding by partitions%'), countIf(explain LIKE '%Partition groups read through separate ports: 4%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.d = r.d);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'parallel_hash');

SELECT 'results match the join without the optimization';
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l INNER JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l INNER JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l LEFT JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l LEFT JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l RIGHT JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l RIGHT JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS join_use_nulls = 1)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS join_use_nulls = 1, query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l INNER JOIN t_right AS r ON l.d = r.d AND l.v % 2 = r.w % 2)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l INNER JOIN t_right AS r ON l.d = r.d AND l.v % 2 = r.w % 2 SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v))) FROM t_left AS l LEFT SEMI JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v))) FROM t_left AS l LEFT SEMI JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v))) FROM t_left AS l LEFT ANTI JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v))) FROM t_left AS l LEFT ANTI JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(r.d, r.w))) FROM t_left AS l RIGHT ANTI JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(r.d, r.w))) FROM t_left AS l RIGHT ANTI JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
-- `ANY` picks an arbitrary one of the matching right rows, so only the columns equal for all of them are compared.
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d))) FROM t_left AS l ANY LEFT JOIN t_right AS r ON l.d = r.d)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d))) FROM t_left AS l ANY LEFT JOIN t_right AS r ON l.d = r.d SETTINGS query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'parallel_hash', max_threads = 16)
  = (SELECT (count(), sum(cityHash64(l.d, l.v, r.d, r.w))) FROM t_left AS l FULL JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'parallel_hash', max_threads = 16, query_plan_join_shard_by_partitions = 0);

SELECT 'runtime filter from the fixed hash table of a single shard';
CREATE TABLE t_small_left (k Int32, v UInt64) ENGINE = MergeTree PARTITION BY toString(k % 6) ORDER BY v;
CREATE TABLE t_small_right (k Int32, v UInt64) ENGINE = MergeTree PARTITION BY toString(k % 6) ORDER BY v;
INSERT INTO t_small_left SELECT (number * 3 + 5) % 97, number FROM numbers(3000);
INSERT INTO t_small_right SELECT (number * 3 + 11) % 97, number FROM numbers(3000);
SELECT
    (SELECT count() FROM (SELECT * FROM t_small_left WHERE k < 50) AS l RIGHT ANTI JOIN t_small_right AS r ON l.k = r.k SETTINGS enable_join_runtime_filters = 1, max_threads = 3)
  = (SELECT count() FROM (SELECT * FROM t_small_left WHERE k < 50) AS l RIGHT ANTI JOIN t_small_right AS r ON l.k = r.k SETTINGS enable_join_runtime_filters = 1, max_threads = 3, query_plan_join_shard_by_partitions = 0);
SELECT
    (SELECT count() FROM t_small_left AS l INNER JOIN t_small_right AS r ON l.k = r.k SETTINGS enable_join_runtime_filters = 1)
  = (SELECT count() FROM t_small_left AS l INNER JOIN t_small_right AS r ON l.k = r.k SETTINGS enable_join_runtime_filters = 1, query_plan_join_shard_by_partitions = 0);
DROP TABLE t_small_left;
DROP TABLE t_small_right;

SELECT 'partition keys of time zones that differ';
CREATE TABLE t_tz_left (d DateTime('UTC'), v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY v;
CREATE TABLE t_tz_right (d DateTime('Asia/Tokyo'), w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;
INSERT INTO t_tz_left SELECT toDateTime(arrayJoin(['2026-01-31 23:30:00', '2026-03-15 12:00:00', '2026-04-15 12:00:00']), 'UTC') AS d, toUInt64(toYYYYMM(d));
INSERT INTO t_tz_right SELECT d, v FROM t_tz_left;
SELECT count() FROM t_tz_left AS l JOIN t_tz_right AS r ON l.d = r.d SETTINGS max_threads = 2;
DROP TABLE t_tz_left;
DROP TABLE t_tz_right;

SELECT 'implicit time zones of the sessions that created the tables';
SET session_timezone = 'UTC';
CREATE TABLE t_tz_left (d DateTime, v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY v;
INSERT INTO t_tz_left SELECT toDateTime(arrayJoin([1769902200, 1773576000, 1776254400])), 1;
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_tz_right (d DateTime, w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;
INSERT INTO t_tz_right SELECT toDateTime(arrayJoin([1769902200, 1773576000, 1776254400])), 1;
SELECT count() FROM t_tz_left AS l JOIN t_tz_right AS r ON l.d = r.d SETTINGS max_threads = 2;
SET session_timezone = DEFAULT;
DROP TABLE t_tz_left;
DROP TABLE t_tz_right;

SELECT 'max_rows_in_join limits the whole right side';
SELECT count() FROM t_left AS l JOIN t_right AS r ON l.d = r.d SETTINGS max_rows_in_join = 150, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'read in order';
CREATE TABLE t_order_left (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY v;
CREATE TABLE t_order_right (k UInt64, w UInt64) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY w;
SYSTEM STOP MERGES t_order_left;
INSERT INTO t_order_left VALUES (0, 1), (2, 3);
INSERT INTO t_order_left VALUES (4, 2), (6, 4);
INSERT INTO t_order_left VALUES (1, 5);
INSERT INTO t_order_right SELECT k, k FROM t_order_left;
SELECT l.v FROM t_order_left AS l LEFT JOIN t_order_right AS r ON l.k = r.k ORDER BY l.v LIMIT 2 SETTINGS max_threads = 2, optimize_read_in_order = 1, read_in_order_use_virtual_row = 0;
DROP TABLE t_order_left;
DROP TABLE t_order_right;

SELECT 'empty side';
SELECT count() FROM t_left AS l LEFT JOIN t_empty AS r ON l.d = r.d;
SELECT count() FROM t_left AS l LEFT JOIN (SELECT * FROM t_right WHERE w > 1000000) AS r ON l.d = r.d;

SELECT 'partitions without a match are not read for INNER';
SELECT count() FROM t_left AS l JOIN t_right AS r ON l.d = r.d SETTINGS log_comment = '05243_inner';
SYSTEM FLUSH LOGS query_log;
-- 3 * 184 rows of 2026-03 .. 2026-08 on the left, 2 * 184 on the right.
SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05243_inner' AND type = 'QueryFinish';

SELECT 'not applied';
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right_month AS r ON l.d = r.d);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right_datetime AS r ON l.d = r.d);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.v = r.w);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.d = r.d OR l.v = r.w);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'full_sorting_merge');
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.d = r.d SETTINGS max_threads = 16);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'parallel_hash', enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);
SELECT countIf(explain LIKE '%Sharding by partitions%') FROM (EXPLAIN actions = 1 SELECT * FROM t_left AS l LEFT JOIN t_right AS r ON l.d = r.d SETTINGS join_algorithm = 'parallel_hash');

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_right_month;
DROP TABLE t_right_datetime;
DROP TABLE t_empty;
