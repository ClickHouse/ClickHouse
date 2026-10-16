SET enable_analyzer = 1;

WITH mergeTreePartInfo('all_12_25_7_4') AS info
SELECT info.partition_id, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('merge-not-byte-identical_all_12_25_7_4_try100') AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('all_12_25_7_4_try100') AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('broken-on-start_all_12_25_7_4') AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('broken-on-start_1_2_3_0', true) AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('broken-on-start_1_2_3_0_try100', true) AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

SELECT mergeTreePartInfo('broken_1_2_1_0', true); -- { serverError 36 }

SELECT mergeTreePartInfo('broken_1_2_3', true); -- { serverError 36 }

SELECT mergeTreePartInfo('broken_1_2_3_try1', true); -- { serverError 36 }

SELECT mergeTreePartInfo('all_1_1_0_try', true); -- { serverError 36 }

SELECT mergeTreePartInfo('broken-on-start_1_2_3_0_tryfoo', true); -- { serverError 36 }

WITH mergeTreePartInfo('broken_1_2_3_4') AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

WITH mergeTreePartInfo('broken_1_2_3_4', false) AS info
SELECT info.partition_id, info.prefix, info.suffix, info.min_block, info.max_block, info.level, info.mutation;

CREATE TABLE mt(key UInt64, value String)
ENGINE = MergeTree
ORDER BY key;

SYSTEM STOP MERGES mt;
INSERT INTO mt SELECT rand(), rand() FROM numbers(4) SETTINGS min_insert_block_size_rows=1, max_block_size=1;
SELECT _part FROM mt ORDER BY mergeTreePartInfo(_part).max_block DESC;

SELECT _part, isMergeTreePartCoveredBy(_part, 'all_1_2_10') FROM mt ORDER BY mergeTreePartInfo(_part).max_block DESC;
