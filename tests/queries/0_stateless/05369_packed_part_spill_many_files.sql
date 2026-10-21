-- The last chunk of each file that is still being written cannot be moved out of memory. In a wide packed part
-- with many files these chunks take more than max_bytes_to_buffer_for_packed_part, so the limit cannot be reached.
-- Each move must still free a large amount of memory, instead of moving every new chunk separately.

DROP TABLE IF EXISTS t_packed_spill_many_files;

-- Every path of the JSON column is a separate file of the wide part.
CREATE TABLE t_packed_spill_many_files (id UInt64, j JSON(max_dynamic_paths = 100))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 1048576;

-- 50 files of about 250 KB each, so the last chunks of the files take about 3 MiB.
INSERT INTO t_packed_spill_many_files
SELECT number, toJSONString(mapFromArrays(
    arrayMap(i -> concat('k', toString(i)), range(50)),
    arrayMap(i -> sipHash64(number, i) % 1000000000000, range(50))))
FROM numbers(50000)
SETTINGS async_insert = 0, max_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0,
    max_insert_block_size = 65536, log_comment = '05369_insert';

SELECT count(), sum(j.k0::Int64) = sum(sipHash64(id, 0) % 1000000000000) FROM t_packed_spill_many_files;
SELECT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_spill_many_files' AND active;

SYSTEM FLUSH LOGS query_log;
-- Each move frees at least half of the limit, but the last chunks of the finalized files are not filled entirely.
SELECT
    ProfileEvents['ExternalPackedPartWriterWritePart'] > 0,
    ProfileEvents['ExternalPackedPartWriterUncompressedBytes'] / ProfileEvents['ExternalPackedPartWriterWritePart'] >= 1048576 / 4
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05369_insert';

DROP TABLE t_packed_spill_many_files;
