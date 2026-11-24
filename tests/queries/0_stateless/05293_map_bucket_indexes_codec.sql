DROP TABLE IF EXISTS t_map_t64_buckets;

CREATE TABLE t_map_t64_buckets (id UInt64, m Map(UInt64, UInt64) CODEC(T64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, serialization_info_version = 'with_types';

INSERT INTO t_map_t64_buckets SELECT number, map(cityHash64(number), number, cityHash64(number + 1), number + 1) FROM numbers(1000);

SELECT substream, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_map_t64_buckets)
WHERE substream IN ('m.bucket_indexes', 'm.buckets_info') ORDER BY substream;
SELECT count(), sum(m[cityHash64(id)]), sum(m[cityHash64(id + 1)]) FROM t_map_t64_buckets;

DROP TABLE t_map_t64_buckets;
