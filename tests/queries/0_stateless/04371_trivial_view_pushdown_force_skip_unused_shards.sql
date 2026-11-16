-- The trivial-view pushdown ships the outer query's WHERE in the view-output namespace, which cannot
-- be safely mapped to the underlying table's sharding key, so it forgoes optimize_skip_unused_shards
-- pruning (filter_actions_dag is cleared after inlining). With force_optimize_skip_unused_shards the
-- inability to prune is a hard error (UNABLE_TO_SKIP_UNUSED_SHARDS), so the pushdown must be
-- suppressed and the query must fall back to the canonical StorageView path, which propagates the
-- outer WHERE to the underlying Distributed table and prunes correctly. The result must be correct and
-- identical with the setting on and off.
--
-- Tags: shard

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS data_04371;
DROP TABLE IF EXISTS dist_04371;
DROP VIEW IF EXISTS view_04371;

CREATE TABLE data_04371 (key Int, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO data_04371 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE dist_04371 AS data_04371
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_04371, key % 2);

CREATE VIEW view_04371 AS SELECT * FROM dist_04371;

SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 1;

-- Pushdown is suppressed under forced pruning: the plan keeps the "VIEW subquery" steps.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT * FROM view_04371 WHERE key = 0
      SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- The query through the view succeeds (no UNABLE_TO_SKIP_UNUSED_SHARDS) and prunes correctly,
-- identical with the pushdown setting on and off.
SELECT * FROM view_04371 WHERE key = 0 ORDER BY key
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT * FROM view_04371 WHERE key = 0 ORDER BY key
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;

DROP VIEW view_04371;
DROP TABLE dist_04371;
DROP TABLE data_04371;
