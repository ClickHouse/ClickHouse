-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: settings randomization changes whether the cache is used at all.

-- When an aggregate projection covers only some parts, the aggregation is rewritten to `AggregatingProjectionStep`,
-- which does not consume plan-time partial aggregate cache hits: the raw parts must still be read.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_pac_mixed_proj;

CREATE TABLE test_pac_mixed_proj (category String, value Int64) ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES test_pac_mixed_proj;

INSERT INTO test_pac_mixed_proj SELECT 'A', number FROM numbers(1000);

-- Not materialized for the existing part.
ALTER TABLE test_pac_mixed_proj ADD PROJECTION p_agg (SELECT category, sum(value) GROUP BY category);

INSERT INTO test_pac_mixed_proj SELECT 'B', number FROM numbers(1000);

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;

SELECT '--- warm without projections';
SELECT category, sum(value) FROM test_pac_mixed_proj GROUP BY category ORDER BY category SETTINGS optimize_use_projections = 0;
SELECT category, sum(value) FROM test_pac_mixed_proj GROUP BY category ORDER BY category SETTINGS optimize_use_projections = 0;

SELECT '--- with the projection on some parts';
SELECT category, sum(value) FROM test_pac_mixed_proj GROUP BY category ORDER BY category
SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p_agg';

DROP TABLE test_pac_mixed_proj;
