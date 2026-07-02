-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- Regression test: in a distributed Cascades plan, `SortedRead` claims the ORDER BY
-- ordering without a merge, so a multi-part table is read as several already-sorted
-- streams. A `LIMIT ... WITH TIES` on top must merge them into one stream first, or
-- `LimitTransform` aborts with "Cannot use LimitTransform with multiple ports and ties".

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET max_threads = 4;

DROP TABLE IF EXISTS t_limit_ties;

CREATE TABLE t_limit_ties (a UInt64) ENGINE = MergeTree() ORDER BY a;
-- Two parts, each value duplicated, so ties straddle the limit boundary and the
-- distributed sorted read produces more than one stream.
INSERT INTO t_limit_ties SELECT number FROM numbers(20);
INSERT INTO t_limit_ties SELECT number FROM numbers(20);

SELECT '-- ASC LIMIT WITH TIES';
SELECT a FROM t_limit_ties ORDER BY a LIMIT 3 WITH TIES;

SELECT '-- DESC LIMIT WITH TIES';
SELECT a FROM t_limit_ties ORDER BY a DESC LIMIT 3 WITH TIES;

SELECT '-- LIMIT WITH TIES + OFFSET';
SELECT a FROM t_limit_ties ORDER BY a LIMIT 2 OFFSET 4 WITH TIES;

DROP TABLE t_limit_ties;
