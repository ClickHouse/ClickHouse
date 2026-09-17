-- Tags: no-random-merge-tree-settings
SET enable_analyzer = 1, explain_query_plan_default = 'legacy';
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 0;
SET convert_query_to_cnf = 0, enable_parallel_replicas = 0;
SET materialize_statistics_on_insert = 0, use_statistics = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS prewhere_compact_pruned;

-- Partition 0 is a wide part, partition 1 is a compact part: compact parts do not publish per-column sizes.
CREATE TABLE prewhere_compact_pruned (p UInt8, x1 Int, x4 String CODEC(NONE))
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 5000, min_bytes_for_wide_part = 0;

INSERT INTO prewhere_compact_pruned SELECT 0, number, repeat('a', 1024) FROM numbers(10000);
INSERT INTO prewhere_compact_pruned SELECT 1, number, repeat('a', 1024) FROM numbers(100);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'prewhere_compact_pruned' AND active ORDER BY partition;

-- Pruning to the compact partition must not lose the column sizes: `x1` is cheap and `x4` is heavy,
-- so `x1` is moved alongside the partition condition and `x4` stays in WHERE, like for the whole table.
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_String', '')
FROM (EXPLAIN actions=1 SELECT count() FROM prewhere_compact_pruned WHERE p = 1 AND x4 > '100' AND x1 > 50)
WHERE explain LIKE '%Prewhere filter column%';

SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_String', '')
FROM (EXPLAIN actions=1 SELECT count() FROM prewhere_compact_pruned WHERE p = 1 AND x4 > '100' AND x1 > 50 SETTINGS use_partition_pruning = 0)
WHERE explain LIKE '%Prewhere filter column%';

SELECT count() FROM prewhere_compact_pruned WHERE p = 1 AND x4 > '100' AND x1 > 50;

DROP TABLE prewhere_compact_pruned;
