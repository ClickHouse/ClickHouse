SET enable_analyzer = 1;

WITH mergeTreePartInfo('all_12_25_7_4') AS info
SELECT info.partition_id, info.min_block, info.max_block, info.level, info.mutation;

CREATE TABLE mt(key UInt64, value String)
ENGINE = MergeTree
ORDER BY key;

SYSTEM STOP MERGES mt;
INSERT INTO mt SELECT rand(), rand() FROM numbers(4) SETTINGS min_insert_block_size_rows=1, max_block_size=1;
SELECT _part FROM mt ORDER BY mergeTreePartInfo(_part).max_block DESC;

SELECT _part, isMergeTreePartCoveredBy(_part, 'all_1_2_10') FROM mt ORDER BY mergeTreePartInfo(_part).max_block DESC;
