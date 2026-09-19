-- Tags: distributed

-- Regression test for a server abort in the local to `GLOBAL` `IN` rewrite applied to `Distributed`
-- reads. The query tree is a DAG and the rewriting visitor keeps no visited set, so a single `IN`
-- node is reached once per referencing parent. The rewrite used to build a replacement node and move
-- the original's arguments into it, which left the original node still named locally but with an
-- empty argument list; the next parent reference then indexed argument [1] of that empty list and
-- libc++'s bounds assertion aborted the server.
--
-- The sharing comes from the null-safe `has` rewrite: `<scalar> GLOBAL IN (<bare column>)` becomes
-- `if(isNull(<scalar>), NULL, has([<bare column>], <scalar>))`, which installs the same `<scalar>`
-- node under both `isNull` and `has`.

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_local_not_null;
DROP TABLE IF EXISTS t_dist_not_null;

CREATE TABLE t_local (idx Int32, i Nullable(UInt64)) ENGINE = MergeTree ORDER BY idx;
INSERT INTO t_local SELECT number, number FROM numbers(16);

CREATE TABLE t_local_not_null (idx Int32, i UInt64) ENGINE = MergeTree ORDER BY idx;
INSERT INTO t_local_not_null SELECT number, number FROM numbers(16);

-- `rand()` as the sharding key, so that a randomized `optimize_skip_unused_shards` cannot prune a shard.
CREATE TABLE t_dist AS t_local
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local, rand());
CREATE TABLE t_dist_not_null AS t_local_not_null
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local_not_null, rand());

SET enable_analyzer = 1;
-- `prefer_global_in_and_join` is the only setting that runs the rewrite this test covers.
SET prefer_global_in_and_join = 1;
-- The shipped shard queries are read back from `system.query_log` below. A local replica is read
-- in process and is never logged, so the TCP path has to be forced (as `03546` does).
SET prefer_localhost_replica = 0;

SELECT 'shared IN node';
-- `transform_null_in = 0` keeps the null-safe rewrite, which is what shares the inner `IN` node.
SELECT count() FROM t_dist WHERE (i NOT IN (SELECT i FROM t_local)) GLOBAL IN (idx)
SETTINGS transform_null_in = 0;

SELECT 'control: transform_null_in collapses the sharing';
SELECT count() FROM t_dist WHERE (i NOT IN (SELECT i FROM t_local)) GLOBAL IN (idx)
SETTINGS transform_null_in = 1;

SELECT 'control: a not-Nullable left operand collapses the sharing';
SELECT count() FROM t_dist_not_null WHERE (i NOT IN (SELECT i FROM t_local_not_null)) GLOBAL IN (idx)
SETTINGS transform_null_in = 0;

SELECT 'control: an unshared IN is still promoted';
SELECT count() FROM t_dist WHERE i NOT IN (SELECT i FROM t_local WHERE i < 8);

-- Every `IN` function of a query shipped to a shard must have been promoted to its `GLOBAL`
-- counterpart, otherwise this test would also pass on a fix that stopped rewriting altogether.
-- Whitespace and case are removed so that the counts do not depend on whether the function is
-- formatted as an operator (`a GLOBAL NOT IN (b)`) or as a call (`globalNotIn(a, b)`).
-- A shard query runs with `current_database` set to `default`, so the rows are selected by
-- `log_comment` instead, as in `03620_analyzer_distributed_global_in`.
-- SKIP: current_database = currentDatabase()
SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(countMatches(q, 'not(null)?in\\(') != countMatches(q, 'globalnot(null)?in\\(')) AS shipped_with_a_local_in,
    countIf(countMatches(q, 'globalnot(null)?in\\(') > 0) > 0 AS shipped_a_promoted_in
FROM
(
    SELECT DISTINCT replaceRegexpAll(lower(query), '\\s+', '') AS q
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND log_comment LIKE '%' || currentDatabase() || '%'
      AND NOT is_initial_query AND type != 'QueryStart' AND query_kind = 'Select'
);

SELECT 'the server is still alive';

DROP TABLE t_dist;
DROP TABLE t_dist_not_null;
DROP TABLE t_local;
DROP TABLE t_local_not_null;
