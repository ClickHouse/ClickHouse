-- Tags: distributed
-- A window function that references no column of a distributed table expression used to make the
-- shard's mergeable-stage header empty, and a block with no column cannot carry a row count, so the
-- rows were silently lost. The analyzer half is covered by 04751; this test covers enable_analyzer=0.
--
-- Every witness query below is bare on purpose: projecting anything alongside the window function,
-- a label string included, makes that column a required output of the window step, the header is
-- then not empty and the query already worked. Labels are therefore separate statements.

SET enable_analyzer = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t04812_loc;
DROP TABLE IF EXISTS t04812_dist;
DROP TABLE IF EXISTS t04812_merge;
DROP VIEW IF EXISTS t04812_num_view;
DROP TABLE IF EXISTS t04812_dist_num;
DROP TABLE IF EXISTS t04812_dist_of_dist;

CREATE TABLE t04812_loc (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t04812_loc SELECT number, toString(number) FROM numbers(3);
CREATE TABLE t04812_dist AS t04812_loc
    ENGINE = Distributed('test_cluster_two_shards_internal_replication', currentDatabase(), 't04812_loc');
CREATE TABLE t04812_merge (a UInt64, b String) ENGINE = Merge(currentDatabase(), '^t04812_dist$');
CREATE VIEW t04812_num_view AS SELECT number AS a FROM numbers(3);
CREATE TABLE t04812_dist_num (a UInt64)
    ENGINE = Distributed('test_cluster_two_shards_internal_replication', currentDatabase(), 't04812_num_view');
-- A Distributed table over a Distributed table: its middle rank both reads from and writes to the
-- mergeable stage, so it is neither the first nor the second stage of the query.
CREATE TABLE t04812_dist_of_dist AS t04812_loc
    ENGINE = Distributed('test_cluster_two_shards_internal_replication', currentDatabase(), 't04812_dist');

SELECT '--- witnesses: every row must be returned. All of them were lost before the fix.';

SELECT 'distributed table';
SELECT count(*) OVER () FROM t04812_dist;

SELECT 'distributed table at the default prefer_localhost_replica, where only the remote shard was lost';
SELECT count(*) OVER () FROM t04812_dist SETTINGS prefer_localhost_replica = 1;

SELECT 'cluster over view';
SELECT count(*) OVER () FROM cluster('test_cluster_two_shards_internal_replication', view(SELECT 1 AS c0));

SELECT 'clusterAllReplicas over view';
SELECT count(*) OVER () FROM clusterAllReplicas('test_cluster_two_shards_internal_replication', view(SELECT 1 AS c0));

-- remote() is not merely a spelling of cluster(): it passes is_remote_function to StorageDistributed,
-- which changes the initial user and the parallel-replicas cluster choice.
SELECT 'remote over view';
SELECT count(*) OVER () FROM remote('127.0.0.{1,2}', view(SELECT 1 AS c0));

SELECT 'Merge over Distributed';
SELECT count(*) OVER () FROM t04812_merge;

SELECT 'distributed over a numbers view';
SELECT count(*) OVER () FROM t04812_dist_num;

SELECT 'distributed over distributed, whose middle rank is neither the first nor the second stage';
SELECT count(*) OVER () FROM t04812_dist_of_dist;

SELECT 'row_number';
SELECT row_number() OVER () FROM t04812_dist;

SELECT 'with ORDER BY';
SELECT count(*) OVER () FROM t04812_dist ORDER BY 1;

SELECT 'with DISTINCT';
SELECT DISTINCT count(*) OVER () FROM t04812_dist;

SELECT 'in a subquery';
SELECT c FROM (SELECT count(*) OVER () AS c FROM t04812_dist) ORDER BY c;

SELECT 'the only column consumed by WHERE';
SELECT count(*) OVER () FROM t04812_dist WHERE a > 0;

SELECT 'the only column consumed by PREWHERE';
SELECT count(*) OVER () FROM t04812_dist PREWHERE a > 0;

SELECT 'union of two such queries';
SELECT c FROM (SELECT count(*) OVER () AS c FROM t04812_dist UNION ALL SELECT count(*) OVER () AS c FROM t04812_dist) ORDER BY c;

SELECT 'asterisk over the window subquery';
SELECT * FROM (SELECT count(*) OVER () FROM t04812_dist) ORDER BY 1;

SELECT '--- nothing internal is visible to the user';

SELECT 'the distributed result has exactly one column, named after the window function';
DESCRIBE (SELECT count(*) OVER () FROM t04812_dist);
SELECT 'and so does the local one';
DESCRIBE (SELECT count(*) OVER () FROM t04812_loc);
SELECT 'a user column may carry the same name as the internal one, with the marker inactive';
SELECT * FROM (SELECT a AS __row_count_marker, count(*) OVER () AS c FROM t04812_dist) ORDER BY 1, 2;
-- Here the only projection is the window function, so the internal column IS added and the user's
-- own output column carries the same name. The user's alias must win.
SELECT 'a window function may be aliased to the internal name';
SELECT count(*) OVER () AS `__row_count_marker` FROM t04812_dist;
-- The row count of a local query is never at risk, so a local plan must not carry the marker at all.
-- Only the plan shows this: the marker is stripped again before the result either way, so no query
-- result can tell a local pipeline that carries it from one that does not.
SELECT 'plan steps naming the internal column, local then distributed';
SELECT countIf(explain LIKE '%__row_count_marker%')
FROM viewExplain('EXPLAIN', 'header = 1', (SELECT count(*) OVER () FROM t04812_loc));
SELECT countIf(explain LIKE '%__row_count_marker%') > 0
FROM viewExplain('EXPLAIN', 'header = 1', (SELECT count(*) OVER () FROM t04812_dist));
-- A query whose header is already non-empty must be sent unchanged. Its rows look the same either
-- way, because the marker is projected away, so only the plan can show the text was left alone.
SELECT 'plan steps for queries that must be sent unchanged';
SELECT countIf(explain LIKE '%__row_count_marker%')
FROM viewExplain('EXPLAIN', 'header = 1', (SELECT a, count(*) OVER () FROM t04812_dist));
SELECT countIf(explain LIKE '%__row_count_marker%')
FROM viewExplain('EXPLAIN', 'header = 1', (SELECT sum(a) OVER () FROM t04812_dist));

SELECT '--- controls: unchanged by the fix, and each already worked before it';

SELECT 'a label projected alongside the window function is enough to keep the header non-empty';
SELECT 'label', count(*) OVER () FROM t04812_dist;
SELECT 'window over a referenced column';
SELECT a, count(*) OVER () FROM t04812_dist ORDER BY a, 2;
SELECT 'window with an argument';
SELECT sum(a) OVER () FROM t04812_dist;
SELECT 'window with an ORDER BY key';
SELECT count(*) OVER (ORDER BY a) FROM t04812_dist ORDER BY 1;
SELECT 'window with a PARTITION BY key';
SELECT count() OVER (PARTITION BY 1) FROM t04812_dist;
SELECT 'aggregation, no window';
SELECT count() FROM t04812_dist;
SELECT 'window over an aggregation';
SELECT count(count()) OVER () FROM t04812_dist GROUP BY a;
SELECT 'window over an aggregating subquery';
SELECT sum(c) OVER () FROM (SELECT count() AS c FROM t04812_dist GROUP BY a);
SELECT 'no window, constant only';
SELECT 1 FROM t04812_dist;
SELECT 'no window, GROUP BY ()';
SELECT 1 FROM t04812_dist GROUP BY ();
SELECT 'no window, plain read';
SELECT a FROM t04812_dist ORDER BY a;
SELECT 'distributed over distributed, referenced column';
SELECT a, count(*) OVER () FROM t04812_dist_of_dist ORDER BY a, 2;
SELECT 'distributed over distributed, plain read';
SELECT a FROM t04812_dist_of_dist ORDER BY a;
SELECT 'local, no distributed boundary';
SELECT count(*) OVER () FROM t04812_loc;
SELECT 'local, referenced column';
SELECT a, count(*) OVER () FROM t04812_loc ORDER BY a, 2;

SELECT '--- the same shapes with the analyzer, which is fixed separately, must be unaffected';

SELECT 'analyzer, referenced column';
SELECT a, count(*) OVER () FROM t04812_dist ORDER BY a, 2 SETTINGS enable_analyzer = 1;
SELECT 'analyzer, local';
SELECT count(*) OVER () FROM t04812_loc SETTINGS enable_analyzer = 1;

DROP TABLE t04812_dist_of_dist;
DROP TABLE t04812_dist_num;
DROP VIEW t04812_num_view;
DROP TABLE t04812_merge;
DROP TABLE t04812_dist;
DROP TABLE t04812_loc;
