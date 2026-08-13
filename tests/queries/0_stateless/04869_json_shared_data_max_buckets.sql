-- Test bucketed shared data of JSON and Array(JSON) columns at the maximum allowed number of
-- buckets (256): every path must be written into the same bucket the sub-column reader looks it up in.

DROP TABLE IF EXISTS t_json_max_buckets_wide;
DROP TABLE IF EXISTS t_json_max_buckets_compact;
DROP TABLE IF EXISTS t_array_json_max_buckets;
SET enable_json_type = 1;
SET output_format_json_quote_64bit_integers = 0;

CREATE TABLE t_json_max_buckets_wide (id UInt64, json JSON(max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map_with_buckets',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    object_shared_data_buckets_for_wide_part = 256;

CREATE TABLE t_json_max_buckets_compact (id UInt64, json JSON(max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
    write_marks_for_substreams_in_compact_parts = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_compact_part = 256;

CREATE TABLE t_array_json_max_buckets (id UInt64, arr Array(JSON(max_dynamic_paths = 0)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 256;

-- p0, p1, p3, p4, p6, p8, p12, p20 and p22 land in buckets 128..254, the other p0..p23 in 0..127.
-- p247 hashes to the last bucket (255), which exists only in the 256-bucket configuration.
INSERT INTO t_json_max_buckets_wide
SELECT number, concat('{', arrayStringConcat(arrayMap(j -> concat('"p', toString(j), '":', toString(number * 100 + j)), arrayPushBack(range(24), 247)), ','), '}')
FROM numbers(100);

INSERT INTO t_json_max_buckets_compact SELECT * FROM t_json_max_buckets_wide ORDER BY id;

INSERT INTO t_array_json_max_buckets
SELECT number, arrayMap(k -> CAST(concat('{', arrayStringConcat(arrayMap(j -> concat('"p', toString(j), '":', toString(number * 100 + j + k)), arrayPushBack(range(24), 247)), ','), '}'), 'JSON(max_dynamic_paths = 0)'), range(3))
FROM numbers(50);

SELECT sum(json.p0.:Int64), sum(json.p1.:Int64), sum(json.p3.:Int64), sum(json.p4.:Int64), sum(json.p6.:Int64),
       sum(json.p8.:Int64), sum(json.p12.:Int64), sum(json.p20.:Int64), sum(json.p22.:Int64), sum(json.p247.:Int64),
       sum(json.p2.:Int64), sum(json.p7.:Int64), sum(json.p11.:Int64)
FROM t_json_max_buckets_wide;

SELECT sum(json.p0.:Int64), sum(json.p1.:Int64), sum(json.p3.:Int64), sum(json.p4.:Int64), sum(json.p6.:Int64),
       sum(json.p8.:Int64), sum(json.p12.:Int64), sum(json.p20.:Int64), sum(json.p22.:Int64), sum(json.p247.:Int64),
       sum(json.p2.:Int64), sum(json.p7.:Int64), sum(json.p11.:Int64)
FROM t_json_max_buckets_compact;

SELECT sum(arraySum(x -> assumeNotNull(x), arr.p0.:Int64)), sum(arraySum(x -> assumeNotNull(x), arr.p1.:Int64)),
       sum(arraySum(x -> assumeNotNull(x), arr.p22.:Int64)), sum(arraySum(x -> assumeNotNull(x), arr.p247.:Int64)),
       sum(arraySum(x -> assumeNotNull(x), arr.p7.:Int64))
FROM t_array_json_max_buckets;

SELECT json FROM t_json_max_buckets_wide ORDER BY id LIMIT 1;
SELECT json FROM t_json_max_buckets_compact ORDER BY id LIMIT 1;
SELECT arr FROM t_array_json_max_buckets ORDER BY id LIMIT 1;

DROP TABLE t_json_max_buckets_wide;
DROP TABLE t_json_max_buckets_compact;
DROP TABLE t_array_json_max_buckets;
