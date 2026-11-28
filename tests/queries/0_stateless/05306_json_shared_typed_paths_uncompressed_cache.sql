-- The uncompressed cache over a read buffer that honours read bounds (packed part storage here, object storage in
-- general): a read that ends at a bound must not be cached as the end of the file. Each read is compared with a
-- Memory table of the type without SHARED.

DROP TABLE IF EXISTS mem;
CREATE TABLE mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
INSERT INTO mem SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(60);

DROP TABLE IF EXISTS packed;
CREATE TABLE packed (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760, min_compress_block_size = 1, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO packed SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(60) SETTINGS max_threads = 1, max_insert_threads = 1;
SELECT part_storage_type, part_type FROM system.parts WHERE database = currentDatabase() AND table = 'packed' AND active;

SET use_uncompressed_cache = 1, use_query_condition_cache = 0, max_threads = 1, optimize_move_to_prewhere = 0;

-- granules 1 and 2 in calls of 3 rows: the pass over the cut chunk reads chunk 2 ahead and stops at the task's last chunk
SELECT 'cut calls', (SELECT arraySort(groupArray((x, toString(json)))) FROM packed WHERE x >= 5 AND x < 12 SETTINGS max_block_size = 3) = (SELECT arraySort(groupArray((x, toString(json)))) FROM mem WHERE x >= 5 AND x < 12);
-- the granule after that task, alone, then reached from the earlier granules, then the whole part
SELECT 'next granule, a path', (SELECT arraySort(groupArray((x, json.a))) FROM packed WHERE x >= 12 AND x < 15) = (SELECT arraySort(groupArray((x, json.a))) FROM mem WHERE x >= 12 AND x < 15);
SELECT 'next granule, the column', (SELECT arraySort(groupArray((x, toString(json)))) FROM packed WHERE x >= 12 AND x < 16) = (SELECT arraySort(groupArray((x, toString(json)))) FROM mem WHERE x >= 12 AND x < 16);
SELECT 'across the bound', (SELECT arraySort(groupArray((x, toString(json)))) FROM packed WHERE x >= 9 AND x < 16) = (SELECT arraySort(groupArray((x, toString(json)))) FROM mem WHERE x >= 9 AND x < 16);
SELECT 'whole part, paths', (SELECT arraySort(groupArray((x, json.a, json.c.size0))) FROM packed) = (SELECT arraySort(groupArray((x, json.a, json.c.size0))) FROM mem);
SELECT 'whole part, the column', (SELECT arraySort(groupArray((x, toString(json)))) FROM packed SETTINGS max_block_size = 7) = (SELECT arraySort(groupArray((x, toString(json)))) FROM mem);
-- the same shape through a path reader: a path in cut calls, then the granule after the task
SELECT 'cut calls, a path', (SELECT arraySort(groupArray((x, json.b))) FROM packed WHERE x >= 29 AND x < 36 SETTINGS max_block_size = 3) = (SELECT arraySort(groupArray((x, json.b))) FROM mem WHERE x >= 29 AND x < 36);
SELECT 'next granule after the path', (SELECT arraySort(groupArray((x, toString(json), json.b))) FROM packed WHERE x >= 36 AND x < 40) = (SELECT arraySort(groupArray((x, toString(json), json.b))) FROM mem WHERE x >= 36 AND x < 40);

DROP TABLE packed;
DROP TABLE mem;
