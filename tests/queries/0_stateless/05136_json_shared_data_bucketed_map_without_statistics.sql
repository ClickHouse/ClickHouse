DROP TABLE IF EXISTS json_shared_bucketed_map;
CREATE TABLE json_shared_bucketed_map (j JSON(max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    propagate_types_serialization_versions_to_nested_types = 1;

-- Preserve runtime `Map` types in shared data instead of parsing them as `JSON` objects.
INSERT INTO json_shared_bucketed_map
SELECT * FROM format(RowBinary, 'j JSON(max_dynamic_paths = 0)', concat(
    unhex('02016d'), formatRowNoNewline('RowBinary', CAST(map('k', toUInt64(1)) AS Dynamic)),
    unhex('0174'), formatRowNoNewline('RowBinary', CAST(map('k', CAST(tuple(toUInt64(2)) AS Tuple(a UInt64))) AS Dynamic))))
SETTINGS input_format_binary_read_json_as_string = 0;
SELECT dynamicType(j.m), j.m, dynamicType(j.t), j.t FROM json_shared_bucketed_map;
SELECT j.m.:`Map(String, UInt64)`['k'], j.t.:`Map(String, Tuple(a UInt64))`['k'].a FROM json_shared_bucketed_map;
DROP TABLE json_shared_bucketed_map;
