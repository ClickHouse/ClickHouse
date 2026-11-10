-- Tags: no-parallel-replicas

SET enable_analyzer=1;
SET optimize_move_to_prewhere=1;
SET query_plan_optimize_prewhere=1;
SET use_constant_folding_in_index_analysis=1;
SET use_statistics_for_part_pruning=0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS partition_skip_minmax_folding;

CREATE TABLE partition_skip_minmax_folding (a UInt64, b UInt64, c UInt64, INDEX c_idx c TYPE minmax GRANULARITY 1)
ENGINE = MergeTree
ORDER BY b
PARTITION BY a
SETTINGS index_granularity = 1;

INSERT INTO partition_skip_minmax_folding VALUES (1, 1, 100), (1, 2, 200), (1, 3, 300);
INSERT INTO partition_skip_minmax_folding VALUES (2, 4, 1000), (2, 5, 2000), (2, 6, 3000);
INSERT INTO partition_skip_minmax_folding VALUES (3, 7, 10000), (3, 8, 20000), (3, 9, 30000);

SELECT '== result ==';
SELECT * FROM partition_skip_minmax_folding WHERE (a = 1 AND c > 500) OR (a = 2 AND c < 500) OR (a = 3 AND c > 50000) ORDER BY a, b;

SELECT '== explain ==';
EXPLAIN indexes = 1 SELECT * FROM partition_skip_minmax_folding WHERE (a = 1 AND c > 500) OR (a = 2 AND c < 500) OR (a = 3 AND c > 50000);

DROP TABLE partition_skip_minmax_folding;
