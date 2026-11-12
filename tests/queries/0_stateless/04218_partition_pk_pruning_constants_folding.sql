-- Tags: no-parallel-replicas

SET enable_analyzer=1;
SET optimize_move_to_prewhere=1;
SET query_plan_optimize_prewhere=1;
SET use_constant_folding_in_index_analysis=1;
SET use_statistics_for_part_pruning=0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS partition_pk_folding;

CREATE TABLE partition_pk_folding (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY b
PARTITION BY a
SETTINGS index_granularity = 1;

INSERT INTO partition_pk_folding VALUES (1, 1), (1, 2), (1, 3);
INSERT INTO partition_pk_folding VALUES (2, 4), (2, 5), (2, 6);
INSERT INTO partition_pk_folding VALUES (3, 7), (3, 8), (3, 9);

SELECT '== result ==';
SELECT * FROM partition_pk_folding WHERE (a = 1 AND b >= 1) OR (a = 2 AND b > 10) OR (a = 3 AND b > 10) ORDER BY a, b;

SELECT '== explain ==';
EXPLAIN indexes = 1 SELECT * FROM partition_pk_folding WHERE (a = 1 AND b >= 1) OR (a = 2 AND b > 10) OR (a = 3 AND b > 10);

DROP TABLE partition_pk_folding;
