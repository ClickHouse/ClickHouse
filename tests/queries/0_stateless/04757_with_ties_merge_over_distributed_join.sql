-- Tags: distributed

-- Reading a Merge table over a Distributed table with a JOIN used to send the shards a query
-- that kept LIMIT ... WITH TIES after its ORDER BY was removed, which the shard's parser
-- rejects with WITH_TIES_WITHOUT_ORDER_BY. test_cluster_two_shards has a genuinely remote
-- shard, so the fragment is really serialized; an all-local cluster does not reproduce it.

DROP TABLE IF EXISTS t_112029;
DROP TABLE IF EXISTS d_112029;
DROP TABLE IF EXISTS m_112029;
DROP TABLE IF EXISTS r_112029;
DROP TABLE IF EXISTS t_wide_112029;
DROP TABLE IF EXISTS d_local_112029;
DROP TABLE IF EXISTS m_local_112029;
DROP TABLE IF EXISTS d_wide_112029;
DROP TABLE IF EXISTS m_wide_112029;
DROP TABLE IF EXISTS t_view_112029;
DROP VIEW IF EXISTS v_112029;
DROP TABLE IF EXISTS d_view_112029;

CREATE TABLE t_112029 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_112029 SELECT number, number % 3 FROM numbers(20);
CREATE TABLE d_112029 (a UInt32, b UInt32)
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_112029);
CREATE TABLE m_112029 (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^d_112029$');
CREATE TABLE r_112029 (b UInt32) ENGINE = MergeTree ORDER BY b;
INSERT INTO r_112029 SELECT number FROM numbers(3);

-- Each value of `a` appears twice because the cluster has two shards reading the same table,
-- so rows 5 and 6 are tied on a = 17 and WITH TIES extends LIMIT 5 to six rows.

SELECT '-- 1 witness: Merge over Distributed + JOIN + WITH TIES';
SELECT m.a FROM m_112029 AS m LEFT JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5 WITH TIES;

SELECT '-- 2 witness, old analyzer';
SELECT m.a FROM m_112029 AS m LEFT JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5 WITH TIES SETTINGS enable_analyzer = 0;

SELECT '-- 3 witness via the merge() table function';
SELECT s.a FROM merge(currentDatabase(), '^d_112029$') AS s LEFT JOIN r_112029 AS r ON s.b = r.b
ORDER BY s.a DESC LIMIT 5 WITH TIES;

SELECT '-- 4 witness with GLOBAL INNER JOIN';
SELECT m.a FROM m_112029 AS m GLOBAL INNER JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5 WITH TIES;

SELECT '-- 5 control: plain Distributed + JOIN already worked';
SELECT d.a FROM d_112029 AS d LEFT JOIN r_112029 AS r ON d.b = r.b
ORDER BY d.a DESC LIMIT 5 WITH TIES;

-- max_threads is pinned only here: at 1 this path concatenates the per-shard streams instead of
-- merging them and answers 19 18 17 16 15, which is a separate pre-existing defect of the no-join
-- path and would mask this arm. Every witness above stays fully randomized.
SELECT '-- 6 control: Merge over Distributed without a JOIN already worked';
SELECT m.a FROM m_112029 AS m ORDER BY m.a DESC LIMIT 5 WITH TIES SETTINGS max_threads = 2;

-- Five rows here against six in arm 1 shows WITH TIES is still applied above the JOIN.
SELECT '-- 7 ties are still honored: plain LIMIT 5 returns five rows';
SELECT m.a FROM m_112029 AS m LEFT JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5;

CREATE TABLE t_wide_112029 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_wide_112029 SELECT number, number % 3 FROM numbers(1000);
CREATE TABLE d_local_112029 (a UInt32, b UInt32)
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_wide_112029);
CREATE TABLE m_local_112029 (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^d_local_112029$');

-- Without a JOIN the child keeps its own ORDER BY, so it also keeps and applies its LIMIT.
-- Returning 1000 rows here means the reset was applied to a child that owns its LIMIT.
SELECT '-- 8 a child without a JOIN keeps its own LIMIT';
SELECT a FROM m_local_112029 ORDER BY a DESC LIMIT 5 SETTINGS enable_analyzer = 1;
SELECT a FROM m_local_112029 ORDER BY a DESC LIMIT 5 SETTINGS enable_analyzer = 0;

CREATE TABLE d_wide_112029 (a UInt32, b UInt32)
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_wide_112029);
CREATE TABLE m_wide_112029 (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^d_wide_112029$');

-- The physical order is the reverse of the requested one, so a child that truncated to its
-- first rows would answer 4 3 2 1 0 instead of the largest values.
SELECT '-- 9 the child must not pre-limit an unordered read';
SELECT m.a FROM m_wide_112029 AS m LEFT JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5;
SELECT m.a FROM m_wide_112029 AS m LEFT JOIN r_112029 AS r ON m.b = r.b
ORDER BY m.a DESC LIMIT 5 WITH TIES;

CREATE TABLE t_view_112029 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_view_112029 SELECT number, number % 3 FROM numbers(20);
CREATE VIEW v_112029 AS SELECT a, b FROM t_view_112029;
CREATE TABLE d_view_112029 (a UInt32, b UInt32)
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), v_112029);

-- The same malformed child also fails on the initiator, before any shard sees it: a local shard
-- is planned by `createLocalPlan`, and `collectFiltersForAnalysis` plans that child at the
-- `Complete` stage, the only planner run that builds a limit step for it, so an orderless child
-- carrying `WITH TIES` raises a `LOGICAL_ERROR` there. `prefer_localhost_replica` is pinned
-- because only the local-shard plan reaches that path and the runner randomizes it. The
-- `ARRAY JOIN` doubles every row, so `a` = 17 is tied and `LIMIT 5` returns six rows.
SELECT '-- 10 witness: the initiator side, no remote shard needed';
SELECT a FROM merge(currentDatabase(), '^d_view_112029$') LEFT ARRAY JOIN [1, 2] AS z
ORDER BY a DESC LIMIT 5 WITH TIES SETTINGS prefer_localhost_replica = 1;

DROP TABLE t_112029;
DROP TABLE d_112029;
DROP TABLE m_112029;
DROP TABLE r_112029;
DROP TABLE t_wide_112029;
DROP TABLE d_local_112029;
DROP TABLE m_local_112029;
DROP TABLE d_wide_112029;
DROP TABLE m_wide_112029;
DROP TABLE d_view_112029;
DROP VIEW v_112029;
DROP TABLE t_view_112029;
