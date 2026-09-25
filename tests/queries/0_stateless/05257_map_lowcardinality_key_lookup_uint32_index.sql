-- Test: m[key] on a Map with LowCardinality keys when a block holds more than 65535 distinct keys.

DROP TABLE IF EXISTS t_map_lc_uint32_index;

-- Pinned: with buckets, the subcolumn read would see only the keys of one bucket.
CREATE TABLE t_map_lc_uint32_index (id UInt64, m Map(LowCardinality(String), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

INSERT INTO t_map_lc_uint32_index SELECT 1, mapFromArrays(arrayMap(x -> concat('k', toString(x)), range(70000)), range(1, 70001));

SELECT dumpColumnStructure(m) LIKE '%LowCardinality(size = 70000, UInt32(size = 70000)%' FROM t_map_lc_uint32_index ORDER BY id;

SELECT 'subcolumns=0', m['k0'], m['k12345'], m['k65535'], m['k69999'], m['absent'] FROM t_map_lc_uint32_index ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'subcolumns=1', m['k0'], m['k12345'], m['k65535'], m['k69999'], m['absent'] FROM t_map_lc_uint32_index ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc_uint32_index;
