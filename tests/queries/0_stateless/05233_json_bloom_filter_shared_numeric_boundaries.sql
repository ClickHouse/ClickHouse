-- Numeric equality must stay exact at integer/float boundaries and for signed zero.
SET allow_experimental_json_bloom_filter_index = 1;
CREATE TABLE json_bf_shared_numeric_boundaries (id UInt64, j JSON(max_dynamic_paths = 0), INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO json_bf_shared_numeric_boundaries
SELECT * FROM format(RowBinary, 'id UInt64, j JSON(max_dynamic_paths = 0)', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740993) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(18446744073709551615) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(18446744073709551616.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-9223372036854775808) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(-9223372036854775808.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(-0.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(9)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(10)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(11)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat32(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(12)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('nan') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(13)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('inf') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(14)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('-inf') AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT 'exact integer', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = 9007199254740993 SETTINGS force_data_skipping_indices = 'bf';
SELECT 'exact float', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toFloat64(9007199254740992) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'uint64 max', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toUInt64(18446744073709551615) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'float above uint64', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toFloat64(18446744073709551616.) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'int64 min', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toInt64(-9223372036854775808) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'zero', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = 0 SETTINGS force_data_skipping_indices = 'bf';
SELECT 'nan', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toFloat64('nan') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'infinity', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toFloat64('inf') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'negative infinity', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x = toFloat64('-inf') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'typed int64 zero', arraySort(groupArray(id)) FROM json_bf_shared_numeric_boundaries WHERE j.x.:Int64 = 0 SETTINGS force_data_skipping_indices = 'bf';
DROP TABLE json_bf_shared_numeric_boundaries;
