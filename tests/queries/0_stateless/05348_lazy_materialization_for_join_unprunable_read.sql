-- Lazy materialization over joins with a MergeTree read that cannot prune its columns: a FINAL read that merges by
-- every column it reads, and a `GraphiteMergeTree` read. Such a read cannot be split, so its columns stay in the main
-- read and cross the LIMIT, while the other side of the join is still read lazily. Every query is followed by the
-- number of lazy reads in its plan, and the results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS rp2_f;
DROP TABLE IF EXISTS rp2_d;
DROP TABLE IF EXISTS rp2_g;

CREATE TABLE rp2_f (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
CREATE TABLE rp2_d (k UInt64, b UInt64, dheavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE rp2_g (Path String, Time DateTime, Value Float64, Version UInt32, k UInt64, g String)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);

SYSTEM STOP MERGES rp2_f;
INSERT INTO rp2_f SELECT number, 1 FROM numbers(1000);
INSERT INTO rp2_f SELECT number * 2, 2 FROM numbers(500);
INSERT INTO rp2_d SELECT number, number % 7, concat('d', toString(number)) FROM numbers(1000);
INSERT INTO rp2_g SELECT toString(number), toDateTime('2026-01-01 00:00:00', 'UTC') + number, number, 1, number, concat('g', toString(number))
FROM numbers(1000);

SELECT '-- a FINAL read whose every column FINAL merges by, joined with a prunable read';
SELECT f.v, d.dheavy FROM rp2_f AS f FINAL JOIN rp2_d AS d ON f.k = d.k ORDER BY d.b, d.k LIMIT 5;
SELECT f.v, d.dheavy FROM rp2_f AS f FINAL JOIN rp2_d AS d ON f.k = d.k ORDER BY d.b, d.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT f.v, d.dheavy FROM rp2_f AS f FINAL JOIN rp2_d AS d ON f.k = d.k ORDER BY d.b, d.k LIMIT 5);

SELECT '-- a GraphiteMergeTree read, joined with a prunable read';
SELECT g.g, d.dheavy FROM rp2_g AS g JOIN rp2_d AS d ON g.k = d.k ORDER BY d.b, d.k LIMIT 5;
SELECT g.g, d.dheavy FROM rp2_g AS g JOIN rp2_d AS d ON g.k = d.k ORDER BY d.b, d.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT g.g, d.dheavy FROM rp2_g AS g JOIN rp2_d AS d ON g.k = d.k ORDER BY d.b, d.k LIMIT 5);

DROP TABLE rp2_f;
DROP TABLE rp2_d;
DROP TABLE rp2_g;
