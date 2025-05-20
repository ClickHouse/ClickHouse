WITH 'all_12_25_7_4' AS part_name
SELECT partPartitionId(part_name), partMinBlock(part_name), partMaxBlock(part_name), partMergeLevel(part_name), partMutationLevel(part_name);

CREATE TABLE mt(key UInt64, value String)
ENGINE = MergeTree
ORDER BY key;

SYSTEM STOP MERGES mt;
INSERT INTO mt SELECT rand(), rand() FROM numbers(4) SETTINGS min_insert_block_size_rows=1, max_block_size=1;
SELECT _part FROM mt ORDER BY partMaxBlock(_part) DESC;

SELECT _part, isPartCoveredBy(_part, 'all_1_2_10') FROM mt ORDER BY partMaxBlock(_part) DESC;
