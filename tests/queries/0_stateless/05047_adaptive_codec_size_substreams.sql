-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: randomized granularity and compress block sizes give tiny blocks where T64 does not win every block.

DROP TABLE IF EXISTS t_size_substreams;
DROP TABLE IF EXISTS t_size_substreams_nested;

CREATE TABLE t_size_substreams
(
    s String,               -- jittered sizes 10..49 -> `.size` goes T64
    e String CODEC(ZSTD(1)) -- explicit codec -> `.size` stays generic-only ZSTD
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4';

INSERT INTO t_size_substreams
    SELECT repeat('x', 10 + cityHash64(number) % 40), toString(number) FROM numbers(150000);
INSERT INTO t_size_substreams
    SELECT repeat('x', 10 + cityHash64(number) % 40), toString(number) FROM numbers(150000, 150000);

OPTIMIZE TABLE t_size_substreams FINAL; -- inserts aren't adaptive, merges are

SELECT 'sizes', substream, arraySort(mapKeys(sumMap(codec_block_counts)))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_substreams)
WHERE substream IN ('s.size', 'e.size')
GROUP BY substream ORDER BY substream;

SELECT 'roundtrip', count(), sum(length(s)), sum(length(e)) FROM t_size_substreams;

CREATE TABLE t_size_substreams_nested (k UInt64, n Nested(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4',
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_rows_to_activate = 1;

INSERT INTO t_size_substreams_nested
    SELECT number, range(cityHash64(number) % 32), range(cityHash64(number) % 32) FROM numbers(150000);
INSERT INTO t_size_substreams_nested
    SELECT number, range(cityHash64(number) % 32), range(cityHash64(number) % 32) FROM numbers(150000, 150000);
OPTIMIZE TABLE t_size_substreams_nested FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT 'nested_merge_algorithm', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_size_substreams_nested' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'nested', substream, arraySort(mapKeys(sumMap(codec_block_counts)))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_substreams_nested)
WHERE substream = 'n.size0'
GROUP BY substream;

SELECT 'nested_roundtrip', count(), sum(length(n.a)), sum(length(n.b)), sum(arraySum(n.a)) FROM t_size_substreams_nested;

DROP TABLE t_size_substreams;
DROP TABLE t_size_substreams_nested;
