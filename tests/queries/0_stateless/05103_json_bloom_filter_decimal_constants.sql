DROP TABLE IF EXISTS json_bf_decimal_constants;
CREATE TABLE json_bf_decimal_constants (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO json_bf_decimal_constants
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt32(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt32(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(18446744073709551615) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-9223372036854775808) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toBool(1) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toBool(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt8(2) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(9)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt8(255) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(10)), unhex('00'))) SETTINGS input_format_binary_read_json_as_string = 0;

-- Decimal constants use exact integer tokens, including unsigned limits and booleans.
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128('42.0', 1) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128('42.0', 1) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128('42.1', 1) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128('42.1', 1) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('18446744073709551615', 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('18446744073709551615', 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('18446744073709551616', 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('18446744073709551616', 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('-9223372036854775808', 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal256('-9223372036854775808', 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128(1, 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128(1, 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128(0, 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x = toDecimal128(0, 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x.:Int32 = toDecimal128(42, 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE j.x.:Int32 = toDecimal128(42, 0) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Int64) = toDecimal128(42, 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Int64) = toDecimal128(42, 0) SETTINGS use_skip_indexes = 0;
-- `Bool` and `UInt8` share a storage type, but the cast maps all nonzero values to one.
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Bool) = 1 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Bool) = 1 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Bool) = toDecimal32(1, 0) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Bool) = toDecimal32(1, 0) SETTINGS use_skip_indexes = 0;
-- A missing path becomes zero after a non-nullable cast.
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Int64) = toDecimal128(0, 0) SETTINGS use_skip_indexes = 1;
SELECT arraySort(groupArray(id)) FROM json_bf_decimal_constants WHERE CAST(j.x AS Int64) = toDecimal128(0, 0) SETTINGS use_skip_indexes = 0;
-- Keep overflow exceptions from integer values that cannot fit the comparison's decimal type.
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal32(1, 0) SETTINGS force_data_skipping_indices = 'bf'; -- { serverError DECIMAL_OVERFLOW }
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal32(1, 0) SETTINGS use_skip_indexes = 0; -- { serverError DECIMAL_OVERFLOW }
TRUNCATE TABLE json_bf_decimal_constants;
INSERT INTO json_bf_decimal_constants
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toDecimal32(42, 0) AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toUInt64(18446744073709551615) SETTINGS force_data_skipping_indices = 'bf'; -- { serverError DECIMAL_OVERFLOW }
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toUInt64(18446744073709551615) SETTINGS use_skip_indexes = 0; -- { serverError DECIMAL_OVERFLOW }
-- A larger constant scale must not hide overflow while rescaling stored values.
TRUNCATE TABLE json_bf_decimal_constants;
INSERT INTO json_bf_decimal_constants
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toDecimal64('9999999999999999.99', 2) AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal64(1, 3) SETTINGS force_data_skipping_indices = 'bf'; -- { serverError DECIMAL_OVERFLOW }
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal64(1, 3) SETTINGS use_skip_indexes = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal128(1, 3) SETTINGS force_data_skipping_indices = 'bf';
SELECT count() FROM json_bf_decimal_constants WHERE j.x = toDecimal128(1, 3) SETTINGS use_skip_indexes = 0;
DROP TABLE json_bf_decimal_constants;
