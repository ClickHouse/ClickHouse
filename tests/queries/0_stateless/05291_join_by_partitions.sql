-- `query_plan_join_shard_by_partitions`: a JOIN of two tables partitioned by the same function of the
-- join keys is executed partition by partition. The results must not change, and the join must be
-- sharded only when equal join keys are guaranteed to be in partitions with the same ID.

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
-- The plan must not depend on randomized settings: the order of the sides and the other sharding pass.
SET query_plan_join_swap_table = 'auto', query_plan_join_shard_by_pk_ranges = 0;
-- A join which may spill to disk (`SpillingHashJoin`) is not sharded.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS jbp_l;
DROP TABLE IF EXISTS jbp_r;

CREATE TABLE jbp_l (d Date, k UInt32, v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY v;
CREATE TABLE jbp_r (d Date, k UInt32, w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;

-- The left table has months 1..6, the right one 3..8, two parts per partition.
INSERT INTO jbp_l SELECT toDate('2026-01-01') + number % 181, number % 7, number FROM numbers(1000);
INSERT INTO jbp_l SELECT toDate('2026-01-01') + number % 181, number % 7, number + 1000 FROM numbers(1000);
INSERT INTO jbp_r SELECT toDate('2026-03-01') + number % 184, number % 5, number FROM numbers(1000);
INSERT INTO jbp_r SELECT toDate('2026-03-01') + number % 184, number % 5, number + 1000 FROM numbers(1000);

SELECT 'plan';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'parallel_hash';
-- Filters and expressions between the read and the join keep the partitions.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM (SELECT d, v FROM jbp_l WHERE v % 3 = 0) AS l LEFT JOIN (SELECT d AS dd, w + 1 AS w FROM jbp_r) AS r ON l.d = r.dd)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
-- A partition key over a column which is not a join key: not sharded.
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.k = r.k)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT 'results';
-- Every query runs with the optimization off and on, the two rows must be equal.
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l LEFT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l LEFT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'parallel_hash';

SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l RIGHT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l RIGHT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k, w)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash', join_use_nulls = 1;

SELECT count(), sum(cityHash64(l.d, v)) FROM jbp_l AS l LEFT ANTI JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, v)) FROM jbp_l AS l LEFT ANTI JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT count(), sum(cityHash64(r.d, w)) FROM jbp_l AS l RIGHT SEMI JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(r.d, w)) FROM jbp_l AS l RIGHT SEMI JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

-- `ANY` picks any of the matching rows, compare only the columns it determines.
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k)) FROM jbp_l AS l ANY LEFT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, l.k, v, r.d, r.k)) FROM jbp_l AS l ANY LEFT JOIN jbp_r AS r ON l.d = r.d AND l.k = r.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

-- Subqueries with filters and expressions over the reads.
SELECT count(), sum(cityHash64(l.d, v, r.dd, r.w)) FROM (SELECT d, v FROM jbp_l WHERE v % 3 = 0) AS l LEFT JOIN (SELECT d AS dd, w + 1 AS w FROM jbp_r) AS r ON l.d = r.dd
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.d, v, r.dd, r.w)) FROM (SELECT d, v FROM jbp_l WHERE v % 3 = 0) AS l LEFT JOIN (SELECT d AS dd, w + 1 AS w FROM jbp_r) AS r ON l.d = r.dd
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

-- Self join.
SELECT count(), sum(cityHash64(a.v, b.v)) FROM jbp_l AS a INNER JOIN jbp_l AS b ON a.d = b.d AND a.k = b.k
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(a.v, b.v)) FROM jbp_l AS a INNER JOIN jbp_l AS b ON a.d = b.d AND a.k = b.k
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

DROP TABLE jbp_l;
DROP TABLE jbp_r;

SELECT 'partition keys that do not line up';

CREATE TABLE jbp_l (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY a % 4 ORDER BY tuple();
CREATE TABLE jbp_r (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY b % 4 ORDER BY tuple();
INSERT INTO jbp_l SELECT number % 10, intDiv(number, 10) % 10 FROM numbers(100);
INSERT INTO jbp_r SELECT number % 10, intDiv(number, 10) % 10 FROM numbers(100);

-- Both partition keys are a function of the join keys, but of different ones: `l.a % 4` and `r.b % 4`
-- where `l.a = r.a`. Rows with equal keys are in different partitions, it must not be sharded.
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.a = r.a AND l.b = r.b)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
-- The keys cross over, so `l.a % 4 = r.b % 4` for joined rows: sharded.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.a = r.b AND l.b = r.a)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

SELECT count(), sum(cityHash64(l.a, l.b, r.a, r.b)) FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.a = r.a AND l.b = r.b
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.a, l.b, r.a, r.b)) FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.a = r.a AND l.b = r.b
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.a, l.b, r.a, r.b)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.a = r.b AND l.b = r.a
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash';
SELECT count(), sum(cityHash64(l.a, l.b, r.a, r.b)) FROM jbp_l AS l FULL JOIN jbp_r AS r ON l.a = r.b AND l.b = r.a
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

DROP TABLE jbp_l;
DROP TABLE jbp_r;

-- The same expression over different time zones puts the same instant in different partitions.
CREATE TABLE jbp_l (t DateTime('UTC')) ENGINE = MergeTree PARTITION BY toYYYYMMDD(t) ORDER BY t;
CREATE TABLE jbp_r (t DateTime('Asia/Tokyo')) ENGINE = MergeTree PARTITION BY toYYYYMMDD(t) ORDER BY t;
INSERT INTO jbp_l SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(100);
INSERT INTO jbp_r SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(100);

SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.t = r.t)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
SELECT count() FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.t = r.t
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

DROP TABLE jbp_l;
DROP TABLE jbp_r;

-- Without an explicit time zone, a `DateTime` takes the time zone in effect when the table is created,
-- and the type name does not show it.
SET session_timezone = 'UTC';
CREATE TABLE jbp_l (t DateTime) ENGINE = MergeTree PARTITION BY toYYYYMMDD(t) ORDER BY t;
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE jbp_r (t DateTime) ENGINE = MergeTree PARTITION BY toYYYYMMDD(t) ORDER BY t;
SET session_timezone = '';
INSERT INTO jbp_l SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(100);
INSERT INTO jbp_r SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(100);

SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.t = r.t)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';
SELECT count() FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.t = r.t
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash';

DROP TABLE jbp_l;
DROP TABLE jbp_r;

SELECT 'runtime filters';

-- A single `Date` key makes the hash table of each shard a fixed hash table. It must not be published as
-- the runtime filter of the whole right side, the other shards would filter out their probe rows.
CREATE TABLE jbp_l (d Date, v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY v;
CREATE TABLE jbp_r (d Date, w UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY w;
INSERT INTO jbp_l SELECT toDate('2026-01-01') + number % 181, number FROM numbers(200000);
INSERT INTO jbp_r SELECT toDate('2026-01-01') + number % 181, number FROM numbers(2000);

SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d)
WHERE explain LIKE '%Sharding%'
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash', enable_join_runtime_filters = 1, join_runtime_filter_from_fixed_hash_table = 1;

SELECT count() FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 0, join_algorithm = 'hash', enable_join_runtime_filters = 1, join_runtime_filter_from_fixed_hash_table = 1;
SELECT count() FROM jbp_l AS l INNER JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'hash', enable_join_runtime_filters = 1, join_runtime_filter_from_fixed_hash_table = 1;
SELECT count() FROM jbp_l AS l RIGHT JOIN jbp_r AS r ON l.d = r.d
SETTINGS query_plan_join_shard_by_partitions = 1, join_algorithm = 'parallel_hash', enable_join_runtime_filters = 1, join_runtime_filter_from_fixed_hash_table = 1;

DROP TABLE jbp_l;
DROP TABLE jbp_r;
