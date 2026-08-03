-- A query can read from more than one `Merge` source. Only one table expression is designated for
-- coordinated reading with parallel replicas, and a replica must recognize the other one as a
-- sibling of the designated leaf and read it as usual, instead of mistaking it for a leaf whose set
-- of underlying tables diverged and failing the query with `SUPPORT_IS_DISABLED`.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

DROP TABLE IF EXISTS t_two_merge_a_1;
DROP TABLE IF EXISTS t_two_merge_a_2;
DROP TABLE IF EXISTS t_two_merge_b_1;
DROP TABLE IF EXISTS t_two_merge_b_2;
DROP TABLE IF EXISTS t_two_merge_a;
DROP TABLE IF EXISTS t_two_merge_b;

CREATE TABLE t_two_merge_a_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_a_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_b_1 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_b_2 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;

INSERT INTO t_two_merge_a_1 SELECT number, number * 2 FROM numbers(500);
INSERT INTO t_two_merge_a_2 SELECT number + 500, number FROM numbers(500);
INSERT INTO t_two_merge_b_1 SELECT number, number * 3 FROM numbers(500);
INSERT INTO t_two_merge_b_2 SELECT number + 500, number * 5 FROM numbers(500);

CREATE TABLE t_two_merge_a ENGINE = Merge(currentDatabase(), '^t_two_merge_a_[12]$');
CREATE TABLE t_two_merge_b ENGINE = Merge(currentDatabase(), '^t_two_merge_b_[12]$');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;

SELECT '-- merge() INNER JOIN merge()';
SELECT count(), sum(l.v), sum(r.w) FROM merge(currentDatabase(), '^t_two_merge_a_[12]$') AS l INNER JOIN merge(currentDatabase(), '^t_two_merge_b_[12]$') AS r ON l.k = r.k;

SELECT '-- Merge table INNER JOIN a Merge table';
SELECT count(), sum(l.v), sum(r.w) FROM t_two_merge_a AS l INNER JOIN t_two_merge_b AS r ON l.k = r.k;

SELECT '-- the same Merge table joined with itself';
SELECT count(), sum(l.v), sum(r.v) FROM t_two_merge_a AS l INNER JOIN t_two_merge_a AS r ON l.k = r.k;

SELECT '-- merge() joined with a Merge table in a subquery';
SELECT sum(cnt), sum(s) FROM (SELECT l.k % 10 AS g, count() AS cnt, sum(r.w) AS s FROM merge(currentDatabase(), '^t_two_merge_a_[12]$') AS l INNER JOIN t_two_merge_b AS r ON l.k = r.k GROUP BY g);

SELECT '-- merge() UNION ALL merge()';
SELECT count(), sum(v) FROM (SELECT v FROM merge(currentDatabase(), '^t_two_merge_a_[12]$') UNION ALL SELECT w AS v FROM merge(currentDatabase(), '^t_two_merge_b_[12]$'));

DROP TABLE t_two_merge_a;
DROP TABLE t_two_merge_b;
DROP TABLE t_two_merge_a_1;
DROP TABLE t_two_merge_a_2;
DROP TABLE t_two_merge_b_1;
DROP TABLE t_two_merge_b_2;
