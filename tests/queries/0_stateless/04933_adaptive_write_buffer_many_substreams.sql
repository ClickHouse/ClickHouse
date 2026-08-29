-- Tags: long, no-distributed-cache
-- - long - writes a part with a few thousand streams
-- - no-distributed-cache - distributed cache does not support adaptive buffers

-- A few Map columns with many buckets write thousands of streams, and one write buffer is
-- allocated per stream. The default min_columns_to_activate_adaptive_write_buffer = 500 must be
-- reached by the stream count, not by the 8 columns, otherwise this INSERT needs several GiB.
-- max_compress_block_size is pinned because it sizes those buffers.

DROP TABLE IF EXISTS t_many_substreams;

CREATE TABLE t_many_substreams
(
    id UInt64,
    m_uint Map(UInt64, String),
    m_int Map(Int32, String),
    m_date Map(Date, UInt64),
    m_uuid Map(UUID, UInt64),
    m_lc Map(LowCardinality(String), UInt64),
    m_arr Array(Map(String, UInt64)),
    m_tuple Tuple(a Map(String, UInt64), b Map(UInt64, String))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
    -- pinned: the runner randomizes this over [0, 1000], and 0 (gate off) or a value <= the 8
    -- columns would make this test assert nothing about the stream count
    min_columns_to_activate_adaptive_write_buffer = 500,
    max_compress_block_size = 8388608,
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    serialization_info_version = 'with_types',
    -- pinned: these decide how many streams the String values inside the Maps get
    string_serialization_version = 'with_size_stream',
    propagate_types_serialization_versions_to_nested_types = 1,
    max_buckets_in_map = 100, map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 1, map_buckets_coefficient = 1.66;

-- Without adaptive write buffers this INSERT needs more than 6 GiB; with them it peaks near 100 MiB.
INSERT INTO t_many_substreams SETTINGS max_memory_usage = '512Mi' VALUES (1, {1:'a',2:'b'}, {-1:'neg',1:'pos'}, {'2024-01-01':10}, {'550e8400-e29b-41d4-a716-446655440000':1}, {'foo':1,'bar':2}, [{'x':1},{'y':2}], ({'k':1}, {7:'v'})), (2, {3:'c'}, {2:'two'}, {'2024-06-15':20}, {'6ba7b810-9dad-11d1-80b4-00c04fd430c8':2}, {'baz':3}, [{'z':3}], ({'m':2}, {8:'w'})), (3, {}, {}, {}, {}, {}, [], ({}, {}));

SELECT count(), sum(length(m_uint)) + sum(length(m_lc)) + sum(length(m_arr)) FROM t_many_substreams;

-- Part type, streams and columns. The stream count must stay far above both 500 and the column
-- count, otherwise the INSERT above says nothing about which quantity the threshold is compared
-- against.
SELECT any(part_type), countDistinct(substream), countDistinct(column)
FROM system.parts_columns
ARRAY JOIN substreams AS substream
WHERE database = currentDatabase() AND table = 't_many_substreams' AND active;

DROP TABLE t_many_substreams;
