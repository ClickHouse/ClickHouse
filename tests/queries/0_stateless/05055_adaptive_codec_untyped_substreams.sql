DROP TABLE IF EXISTS t_untyped_substreams;

CREATE TABLE t_untyped_substreams (m Map(UInt64, UInt8))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, serialization_info_version = 'with_types',
         map_serialization_version = 'with_buckets', map_buckets_strategy = 'constant', max_buckets_in_map = 32,
         map_buckets_min_avg_size = 0;

INSERT INTO t_untyped_substreams SELECT map(number * 2, 1, number * 2 + 1, 2) FROM numbers(30);

OPTIMIZE TABLE t_untyped_substreams FINAL; -- inserts aren't adaptive, merges are

SELECT mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_untyped_substreams)
WHERE substream = 'm.bucket_indexes';

SELECT count(), countIf(mapValues(m) = [1, 2]) FROM t_untyped_substreams;

DROP TABLE t_untyped_substreams;
