-- Tags: no-parallel-replicas, no-shared-catalog

SET enable_analyzer=1;
SET optimize_move_to_prewhere=1;
SET query_plan_optimize_prewhere=1;
SET use_constant_folding_in_index_analysis=1;

DROP TABLE IF EXISTS partition_id_with_minmax_folding;

CREATE TABLE partition_id_with_minmax_folding (p UInt64, a UInt64)
ENGINE = MergeTree
ORDER BY a
PARTITION BY p
SETTINGS
    index_granularity = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

SYSTEM STOP MERGES partition_id_with_minmax_folding;
INSERT INTO partition_id_with_minmax_folding SELECT 1, 1; -- 1_1_1_0
INSERT INTO partition_id_with_minmax_folding SELECT 1, 2; -- 1_2_2_0
INSERT INTO partition_id_with_minmax_folding SELECT 2, 1; -- 2_3_3_0
INSERT INTO partition_id_with_minmax_folding SELECT 2, 2; -- 2_4_4_0
INSERT INTO partition_id_with_minmax_folding SELECT 2, 3; -- 2_5_5_0
INSERT INTO partition_id_with_minmax_folding SELECT 2, 4; -- 2_6_6_0

SELECT '== result ==';
SELECT * FROM partition_id_with_minmax_folding WHERE (_partition_id = '1' AND _block_number > 1) or (_partition_id = '2' AND _block_number >= 6) ORDER BY p;

SELECT '== explain ==';
EXPLAIN indexes = 1 SELECT * FROM partition_id_with_minmax_folding WHERE (_partition_id = '1' AND _block_number > 1) or (_partition_id = '2' AND _block_number >= 6);

DROP TABLE partition_id_with_minmax_folding;
