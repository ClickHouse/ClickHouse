-- Tags: distributed, no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: randomized settings and part counts
-- change both the plans and the values these arms count.

SET explain_query_plan_default = 'legacy';
-- max_threads is pinned because the cost heuristic accepts a fixture only when its partition
-- count is at least max_threads / 2; arms that must not depend on the heuristic force it instead.
SET max_threads = 8;
SET enable_parallel_replicas = 0;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
-- The stateless CI profile sets these to 10G, and a nonzero limit disables per-partition
-- evaluation outright.
SET max_rows_to_group_by = 0;
SET max_rows_to_sort = 0;
SET max_bytes_to_sort = 0;
SET optimize_use_implicit_projections = 0;
-- The values and plans below are the analyzer's, so pin it.
SET enable_analyzer = 1;

-- { echo }

-- per-partition set building reads the same predicate. The set fill deduplicates across
-- partitions anyway, so the merged answer stays correct and only the plan shape shows the
-- decline; the bare-key arm is the control that the fixture reaches the optimization.
DROP TABLE IF EXISTS t_set_month;
CREATE TABLE t_set_month (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY d;
SYSTEM STOP MERGES t_set_month;
INSERT INTO t_set_month SELECT toDate(concat(toString(2001 + intDiv(number, 300)), '-01-', toString(29 + (intDiv(number, 100) % 3)))) AS d, number FROM numbers_mt(5100) WHERE toYear(d) NOT IN (2004, 2008, 2012, 2016, 2020);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE toDate('2001-02-28') + number IN (SELECT d + INTERVAL 1 MONTH FROM t_set_month) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE toDate('2001-02-28') + number IN (SELECT d FROM t_set_month) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_set_month;

-- an integer key keeps per-partition set building; the partition key is a function of the set's
-- own output column, which the interval arm above cannot use because its key is the collapsing one
DROP TABLE IF EXISTS t_set_int;
CREATE TABLE t_set_int (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES t_set_int;
INSERT INTO t_set_int SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO t_set_int SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a + 1 FROM t_set_int) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a + 1 FROM t_set_int) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a + 1 FROM t_set_int) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE t_set_int;

-- ---------------------------------------------------------------------------
-- The distributed sharding-key consumer reaches the same predicate through its own rejection
-- loop and its own direct call, so it gets its own arms. Dropping the merge step is only
-- correct when the group key determines the shard: a key that collapses distinct shard-key
-- values leaves each shard's partial groups unmerged, so the same key is returned twice.
-- Each view filters itself by shardNum() so the two shards hold the disjoint rows the
-- declared key implies - a declared key alone does not redistribute rows on a read, and
-- without the filter every shard holds every row and even a sound merge drop doubles the
-- answer. The first arm of each pair counts merge steps (1 = kept, 0 = dropped) and the
-- second compares the answer against the unoptimized one; the integer pair is the control
-- that the optimization still fires where it is sound.
-- ---------------------------------------------------------------------------

SELECT shardNum() AS s, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE toYYYYMMDD(toDate('2001-01-29') + (number % 3)) % 2 = (shardNum() - 1)), toUInt64(toYYYYMMDD(d))) GROUP BY s ORDER BY s;
SELECT count() FROM (EXPLAIN SELECT d + INTERVAL 1 MONTH AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE toYYYYMMDD(toDate('2001-01-29') + (number % 3)) % 2 = (shardNum() - 1)), toUInt64(toYYYYMMDD(d))) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1) WHERE explain ILIKE '%MergingAggregated%';
SELECT (SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE toYYYYMMDD(toDate('2001-01-29') + (number % 3)) % 2 = (shardNum() - 1)), toUInt64(toYYYYMMDD(d))) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 0)) = (SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE toYYYYMMDD(toDate('2001-01-29') + (number % 3)) % 2 = (shardNum() - 1)), toUInt64(toYYYYMMDD(d))) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1));
SELECT shardNum() AS s, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE number % 2 = (shardNum() - 1)), toUInt64(x)) GROUP BY s ORDER BY s;
SELECT count() FROM (EXPLAIN SELECT x + 1 AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE number % 2 = (shardNum() - 1)), toUInt64(x)) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1) WHERE explain ILIKE '%MergingAggregated%';
SELECT (SELECT count() FROM (SELECT x + 1 AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE number % 2 = (shardNum() - 1)), toUInt64(x)) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 0)) = (SELECT count() FROM (SELECT x + 1 AS k, count() FROM remote('127.{1,2}', view(SELECT toDate('2001-01-29') + (number % 3) AS d, number AS x FROM numbers(30) WHERE number % 2 = (shardNum() - 1)), toUInt64(x)) GROUP BY k SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1));

-- ---------------------------------------------------------------------------
-- The window consumer reaches the same predicate through the stream-disjointness
-- propagation, at two sites: the per-partition read request and the scatter skip above it.
-- INTERVAL MONTH collapses the 29th, 30th and 31st into one key, so one logical window
-- partition spans the table partitions those days live in and must not be evaluated per
-- table partition. The default arm carries no setting: the cost heuristic accepts this
-- fixture, so the answer has to be right without opting out.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS t_win_month;
CREATE TABLE t_win_month (d Date) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_win_month SELECT toDate(concat(toString(2001 + intDiv(number, 300)), '-01-', toString(29 + (intDiv(number, 100) % 3)))) AS d FROM numbers_mt(5100) WHERE toYear(d) NOT IN (2004, 2008, 2012, 2016, 2020);
-- the collapse the arms below depend on: more table partitions than window keys
SELECT uniqExact(_partition_id), uniqExact(d + INTERVAL 1 MONTH) FROM t_win_month;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY d + INTERVAL 1 MONTH) AS c FROM t_win_month) ORDER BY c SETTINGS force_window_partitions_independently = 1;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY d + INTERVAL 1 MONTH) AS c FROM t_win_month) ORDER BY c SETTINGS allow_window_partitions_independently = 0;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY d + INTERVAL 1 MONTH) AS c FROM t_win_month) ORDER BY c;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY d + INTERVAL 1 MONTH) FROM t_win_month SETTINGS force_window_partitions_independently = 1) WHERE explain ILIKE '%Read each partition through separate port: 1%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY d + INTERVAL 1 MONTH) FROM t_win_month SETTINGS force_window_partitions_independently = 1) WHERE explain ILIKE '%Skip scatter by partition: 1%';
DROP TABLE t_win_month;

-- the integer control: an injective addend keeps both sites firing, so the arms above
-- attribute to the operand type and not to the window shape or the fixture
DROP TABLE IF EXISTS t_win_int;
CREATE TABLE t_win_int (x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY x % 8;
INSERT INTO t_win_int SELECT number % 8 FROM numbers_mt(800);
SELECT uniqExact(_partition_id), uniqExact(x + 1) FROM t_win_int;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY x + 1) AS c FROM t_win_int) ORDER BY c SETTINGS force_window_partitions_independently = 1;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY x + 1) AS c FROM t_win_int) ORDER BY c SETTINGS allow_window_partitions_independently = 0;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY x + 1) FROM t_win_int SETTINGS force_window_partitions_independently = 1) WHERE explain ILIKE '%Read each partition through separate port: 1%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY x + 1) FROM t_win_int SETTINGS force_window_partitions_independently = 1) WHERE explain ILIKE '%Skip scatter by partition: 1%';
DROP TABLE t_win_int;
