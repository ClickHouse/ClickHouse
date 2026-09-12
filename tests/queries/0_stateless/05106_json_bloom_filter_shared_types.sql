DROP TABLE IF EXISTS json_bf_shared_types;
CREATE TABLE json_bf_shared_types (id UInt64, j JSON(max_dynamic_paths = 0), INDEX bf j TYPE jsonbf_v1() GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Preserve runtime types in shared data, including scalars and recursive values.
INSERT INTO json_bf_shared_types
SELECT * FROM format(RowBinary, 'id UInt64, j JSON(max_dynamic_paths = 0)', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST('abc' AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFixedString('abc', 3) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt8(-1) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(18446744073709551615) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat32(1.25) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(-0.0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toDecimal64('1.5', 1) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', CAST([1, 2] AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(9)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(tuple('abc', toUInt64(1)) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(10)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(map('k', 'abc') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(11)), unhex('010178'), formatRowNoNewline('RowBinary', CAST('' AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT id, dynamicType(j.x), JSONSharedDataPaths(j), JSONDynamicPaths(j) FROM json_bf_shared_types ORDER BY id;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:String = 'abc' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:String = 'abc' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:`FixedString(3)` = toFixedString('abc', 3) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:`FixedString(3)` = toFixedString('abc', 3) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Int8 = -1 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Int8 = -1 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:UInt64 = 18446744073709551615 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:UInt64 = 18446744073709551615 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Float32 = 1.25 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Float32 = 1.25 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Float64 = 0 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:Float64 = 0 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:`Decimal(18, 1)` = toDecimal64('1.5', 1) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:`Decimal(18, 1)` = toDecimal64('1.5', 1) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE has(j.x.:`Array(UInt8)`, 2) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE has(j.x.:`Array(UInt8)`, 2) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:String = '' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_types WHERE j.x.:String = '' SETTINGS use_skip_indexes = 0;
DROP TABLE json_bf_shared_types;
