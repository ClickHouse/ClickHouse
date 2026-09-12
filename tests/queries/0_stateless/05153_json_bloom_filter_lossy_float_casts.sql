CREATE TABLE json_bf_lossy_float_casts (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Preserve both signed and unsigned runtime types around the exact-integer limit of `Float64`.
INSERT INTO json_bf_lossy_float_casts
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740993) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(9007199254740993) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-9007199254740993) AS Dynamic))))
SETTINGS input_format_binary_read_json_as_string = 0;

SELECT id, dynamicType(j.x) FROM json_bf_lossy_float_casts ORDER BY id;
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = toFloat64(9007199254740992) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = toFloat64(9007199254740992) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = '9007199254740992' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = '9007199254740992' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = toFloat64(-9007199254740992) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_lossy_float_casts WHERE CAST(j.x AS Float64) = toFloat64(-9007199254740992) SETTINGS use_skip_indexes = 0;

DROP TABLE json_bf_lossy_float_casts;
