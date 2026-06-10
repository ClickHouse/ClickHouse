-- Shuffle exchanges created by the Cascades `DistributionEnforcer` must align
-- join key types across both sides (least supertype), as the legacy
-- `makeDistributed` path does via `hash_cast_types`. Otherwise `UInt32` keys
-- on one side and `UInt64` on the other hash to different buckets and
-- matching rows land on different nodes.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Pin the shuffle join choice so the test keeps covering the fix.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":0.01,"sequential_weight":100000}';

DROP TABLE IF EXISTS t_keys32;
DROP TABLE IF EXISTS t_keys64;

CREATE TABLE t_keys32 (k Int32, v UInt64) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_keys64 (k Int64, v UInt64) ENGINE = MergeTree() ORDER BY k;

-- Both sides large enough that a shuffle join beats broadcast and local.
INSERT INTO t_keys32 SELECT toInt32(number) - 50000, number FROM numbers(100000);
INSERT INTO t_keys64 SELECT toInt64(number) - 50000, number * 2 FROM numbers(100000);

SELECT '-- 1. Shuffle join with mismatched signed key types: all 100000 keys must match';
SELECT count(), sum(l.v + r.v) FROM t_keys32 AS l JOIN t_keys64 AS r ON l.k = r.k;

SELECT '-- 2. Baseline without Cascades';
SELECT count(), sum(l.v + r.v) FROM t_keys32 AS l JOIN t_keys64 AS r ON l.k = r.k
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- The merge join is not currently electable for mismatched-type keys (the rule skips
-- them fail-close), so this guards against a future cost-model change, not a live path.
SELECT '-- 3. Merge-join-friendly shape (sorted small tables): types still must not split buckets';
SELECT count(), sum(l.v + r.v) FROM (SELECT * FROM t_keys32 WHERE k < -40000) AS l JOIN t_keys64 AS r ON l.k = r.k;

DROP TABLE t_keys32;
DROP TABLE t_keys64;
