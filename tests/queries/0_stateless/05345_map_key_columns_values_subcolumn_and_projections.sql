-- `m.values` of a `with_key_columns` Map, and projections rebuilt by a mutation that
-- writes keys outside the source part's key set.

SET allow_experimental_map_key_columns = 1;

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_kc_values;
CREATE TABLE t_kc_values (id UInt64, m Map(String, UInt64), am Array(Map(String, UInt64))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_kc_values SELECT number, map('a', number, 'b', number * 2), [map('c', number)] FROM numbers(5);
SELECT m.values, mapValues(m), am.values FROM t_kc_values ORDER BY id;
SELECT count() FROM t_kc_values WHERE has(m.values, 4);
SELECT count() FROM t_kc_values PREWHERE has(m.values, 4);
DROP TABLE t_kc_values;

DROP TABLE IF EXISTS t_kc_values_compact;
CREATE TABLE t_kc_values_compact (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', min_bytes_for_wide_part = '10G';
INSERT INTO t_kc_values_compact SELECT number, map('a', number) FROM numbers(3);
SELECT m.values FROM t_kc_values_compact ORDER BY id;
DROP TABLE t_kc_values_compact;

DROP TABLE IF EXISTS t_kc_projections;
CREATE TABLE t_kc_projections
(
    id UInt64,
    g UInt64,
    m Map(String, UInt64),
    PROJECTION p_agg (SELECT g, count(), sum(id) GROUP BY g),
    PROJECTION p_ord (SELECT id, g, m ORDER BY g)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', min_bytes_for_wide_part = 0, index_granularity = 100, merge_max_block_size = 100;
INSERT INTO t_kc_projections SELECT number, number % 3, map('a', number) FROM numbers(1000);
ALTER TABLE t_kc_projections UPDATE m = map(concat('k', toString(intDiv(id, 50))), id), g = g + 10 WHERE 1
SETTINGS min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0;
SELECT g, count(), sum(id) FROM t_kc_projections GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p_agg';
SELECT g, count(), sum(id) FROM t_kc_projections GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 0;
SELECT count(), uniqExact(arrayJoin(mapKeys(m))) FROM mergeTreeProjection(currentDatabase(), t_kc_projections, 'p_ord');
DROP TABLE t_kc_projections;
