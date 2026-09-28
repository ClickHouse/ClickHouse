-- Tags: distributed

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

-- A window function referencing no column of the table expression leaves the shard-side
-- mergeable-stage header with no column of its own, which cannot carry a row count.

SELECT '--- const projection through a table function';
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT 1 AS a));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT 'x' AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT toNullable('s') AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT toLowCardinality('lc') AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT [1, 2] AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT (1, 'a') AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT toDecimal64(1.5, 2) AS c0));
SELECT count(*) OVER () FROM remote('127.0.0.{1,2}', view(SELECT 'r' AS c0));
-- clusterAllReplicas discards duplicate host:port pairs, so a cluster whose two shards are both
-- 127.0.0.1 would collapse into one and read a single row on either side of this fix.
SELECT count(*) OVER () FROM clusterAllReplicas('test_cluster_two_shards', view(SELECT 'r' AS c0));

SELECT '--- const projection through a stored view';
DROP VIEW IF EXISTS v_const;
CREATE VIEW v_const AS SELECT 'vv' AS c0;
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), 'v_const');

SELECT '--- plain table, no constant anywhere';
DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_plain (k UInt8) ENGINE = MergeTree ORDER BY k AS SELECT 1;
DROP TABLE IF EXISTS d_plain;
CREATE TABLE d_plain AS t_plain ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 't_plain');
SELECT count(*) OVER () FROM d_plain;
SELECT count(*) OVER () FROM d_plain SETTINGS serialize_query_plan = 1;
SELECT count(*) OVER () FROM remote('127.0.0.{1,2}', currentDatabase(), 't_plain');

SELECT '--- the column read to count rows differs between the two sides';
-- The initiator plans against a StorageDummy, which reports no column sizes and therefore ranks
-- by type size (`jitter`), while the shard ranks by compressed size (`zeros`).
DROP TABLE IF EXISTS t_sizes;
CREATE TABLE t_sizes (jitter UInt8, zeros UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sizes SELECT rand() % 256, 0 FROM numbers(100000);
SELECT argMin(column, column_data_compressed_bytes) = 'zeros' AND argMax(column, column_data_compressed_bytes) = 'jitter'
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sizes' AND active;
DROP TABLE IF EXISTS d_sizes;
CREATE TABLE d_sizes AS t_sizes ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 't_sizes');
SELECT DISTINCT count(*) OVER () FROM d_sizes;

SELECT '--- UNION ALL and JOIN';
SELECT count(*) OVER () FROM d_plain UNION ALL SELECT count(*) OVER () FROM d_plain;
SELECT count(*) OVER () FROM d_plain AS l JOIN t_plain AS r ON 1 = 1;

SELECT '--- nothing internal reaches the user';
SELECT * FROM (SELECT count(*) OVER () AS w FROM d_plain);
SELECT * APPLY(toString) FROM (SELECT count(*) OVER () AS w FROM cluster('test_cluster_two_shards_localhost', view(SELECT 1 AS a)));
DROP TABLE IF EXISTS t_ins;
CREATE TABLE t_ins (v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ins SELECT count(*) OVER () FROM d_plain;
SELECT count(), sum(v) FROM t_ins;

SELECT '--- a user alias that spells an internal column identifier';
SELECT count(*) OVER () AS `__table1.k` FROM d_plain;
SELECT count(*) OVER () AS `__table1.k` FROM t_plain;
SELECT 1 AS `__table1.k` FROM t_plain;
SELECT count(*) OVER () AS `__row_count_marker` FROM d_plain;

SELECT '--- controls: unchanged shapes';
SELECT count(*) OVER () FROM d_plain SETTINGS prefer_localhost_replica = 1;
SELECT count(k) OVER () FROM d_plain;
SELECT count(*) OVER (PARTITION BY k) FROM d_plain;
SELECT count(*) OVER (ORDER BY k) FROM d_plain;
SELECT 1 FROM d_plain;
SELECT 'c' FROM d_plain;
SELECT count(*) FROM d_plain;
SELECT count(*) OVER () FROM cluster('test_shard_localhost', currentDatabase(), 't_plain');
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT materialize('m') AS c0));
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_localhost', view(SELECT number FROM numbers(3)));
SELECT count(*) OVER () FROM t_plain;
SELECT count(*) OVER () FROM (SELECT count(*) AS c FROM d_plain GROUP BY k);
SELECT count(*) OVER () FROM d_plain SETTINGS distributed_group_by_no_merge = 1;

DROP TABLE t_ins;
DROP TABLE d_sizes;
DROP TABLE t_sizes;
DROP TABLE d_plain;
DROP TABLE t_plain;
DROP VIEW v_const;
