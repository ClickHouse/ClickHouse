-- `m['c']` throws `UNKNOWN_ELEMENT_OF_ENUM` when `c` names no member of the `Enum` key type
-- (see `04513_array_element_enum_key_map`). A `bloom_filter` index on `mapValues(m)` does not look at the key,
-- so it could prune every granule by the value alone and hide that exception. It must decline the atom instead.

DROP TABLE IF EXISTS t_enum_map_key_values_index;

CREATE TABLE t_enum_map_key_values_index
(
    m Map(Enum8('a' = 1, 'b' = 2), String),
    INDEX idx_m mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_enum_map_key_values_index VALUES (map('a', 'v'));

SELECT count() FROM t_enum_map_key_values_index WHERE m['c'] = 'x' SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_map_key_values_index WHERE m['c'] = 'x' SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_map_key_values_index WHERE m['c'] IN ('x', 'y') SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_map_key_values_index WHERE m['c'] IN ('x', 'y') SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- A member key still lets the index prune by the value.
SELECT count() FROM t_enum_map_key_values_index WHERE m['b'] = 'x' SETTINGS force_data_skipping_indices = 'idx_m';
SELECT count() FROM t_enum_map_key_values_index WHERE m['a'] = 'v' SETTINGS force_data_skipping_indices = 'idx_m';
SELECT count() FROM t_enum_map_key_values_index WHERE m['b'] IN ('x', 'y') SETTINGS force_data_skipping_indices = 'idx_m';

DROP TABLE t_enum_map_key_values_index;
