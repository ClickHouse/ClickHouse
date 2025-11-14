SET optimize_if_transform_strings_to_enum=0;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1, object_serialization_version = 'v3', object_shared_data_serialization_version_for_zero_level_parts = 'advanced', object_shared_data_buckets_for_wide_part = 1, index_granularity=2;

INSERT INTO t0 SELECT multiIf(
  number < 2,
  '{"arr" : [{"arr1" : 9}]}',
  '{"a" : {"b" : [{"c" : 42}]}}'
) FROM numbers(3);

SELECT c0.arr.:`Array(JSON)`, c0.^a FROM t0;
DROP TABLE t0;

