-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105855
--
-- Before the fix, the new analyzer did not push an outer `WHERE` through a
-- subquery containing a `LEFT JOIN` onto a `Distributed` (or `remote(...)`)
-- table — the predicate stayed on the initiator, so every shard ran the
-- full join over its entire data before the predicate filtered the result.
--
-- The bailout was in `addFilters` in `ReadFromRemote.cpp` ("Case with JOIN
-- is not supported so far."). The fix lets the predicate ride into the
-- subquery sent to shards as a `HAVING`, which the downstream analyzer
-- promotes to `WHERE` and pushes through the JOIN onto the eligible side.

-- Tags: no-random-merge-tree-settings, no-parallel-replicas

SET enable_analyzer = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET prefer_localhost_replica = 0;
SET enable_parallel_replicas = 0;
SET joined_subquery_requires_alias = 0;

DROP TABLE IF EXISTS t_left_join_pushdown_left;
DROP TABLE IF EXISTS t_left_join_pushdown_right;

CREATE TABLE t_left_join_pushdown_left
(
    col1 UInt64,
    col2 UInt32,
    col3 String,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY (timestamp);

CREATE TABLE t_left_join_pushdown_right
(
    col2 UInt32,
    lookup String
)
ENGINE = MergeTree
ORDER BY col2;

INSERT INTO t_left_join_pushdown_left
SELECT number, number % 100, 'a' || toString(number % 1000), toDateTime('2026-05-20 00:00:00') + INTERVAL number SECOND
FROM numbers(10000);

INSERT INTO t_left_join_pushdown_right
SELECT number, 'lk' || toString(number) FROM numbers(500);

-- Run the buggy form (outer WHERE on a subquery containing a JOIN to
-- a remote table). After the fix, the SQL that ClickHouse serializes
-- to remote shards must include a HAVING/WHERE that carries the
-- outer-query predicate. Before the fix, the remote received the
-- unfiltered `SELECT * FROM left LEFT JOIN right` and ran the full
-- join over its entire dataset before the initiator filtered.
SET log_queries = 1;
SELECT col1
FROM (
    SELECT *
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_left_join_pushdown_left)
    LEFT JOIN t_left_join_pushdown_right USING (col2)
)
WHERE timestamp >= '2026-05-20 00:00:10'
  AND timestamp < '2026-05-20 00:00:20'
  AND col3 = 'a50'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Inspect the SQL the initiator actually sent to the shards.
-- After the fix it must contain the outer-query predicate (visible
-- through any of the column names referenced by the WHERE — we use
-- `col3` and the `'a50'` literal since either alone is specific enough
-- to be unambiguous in this test).
SELECT 'remote_subquery_has_predicate';
SELECT
    countIf(query ILIKE '%a50%') > 0 AND countIf(query ILIKE '%LEFT JOIN%') > 0
        AS remote_query_carries_outer_where
FROM system.query_log
WHERE type = 'QueryStart'
  AND is_initial_query = 0
  AND query ILIKE concat('%', currentDatabase(), '%t_left_join_pushdown_left%')
  AND event_time >= now() - INTERVAL 5 MINUTE;

DROP TABLE t_left_join_pushdown_left;
DROP TABLE t_left_join_pushdown_right;
