-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test serialization functionality
SELECT '=== Serialization functionality ===';

-- Test JSON serialization
SELECT '=== JSON serialization ===';
SELECT toJSONString(toDecimal512('123.456', 3)) AS json_string;
SELECT toJSONString(toDecimal512('-123.456', 3)) AS json_string_negative;
SELECT toJSONString(toDecimal512('0.0', 3)) AS json_string_zero;

-- Test CSV format
SELECT '=== CSV format ===';
SELECT formatRow('CSV', toDecimal512('123.456', 3)) AS csv_row;
SELECT formatRow('CSV', toDecimal512('-123.456', 3)) AS csv_row_negative;
SELECT formatRow('CSV', toDecimal512('0.0', 3)) AS csv_row_zero;

-- Test TSV format
SELECT '=== TSV format ===';
SELECT formatRow('TSV', toDecimal512('123.456', 3)) AS tsv_row;
SELECT formatRow('TSV', toDecimal512('-123.456', 3)) AS tsv_row_negative;
SELECT formatRow('TSV', toDecimal512('0.0', 3)) AS tsv_row_zero;

-- Test Values format
SELECT '=== Values format ===';
SELECT formatRow('Values', toDecimal512('123.456', 3)) AS values_row;
SELECT formatRow('Values', toDecimal512('-123.456', 3)) AS values_row_negative;
SELECT formatRow('Values', toDecimal512('0.0', 3)) AS values_row_zero;

-- Test with different scales
SELECT '=== Different scales serialization ===';
SELECT toJSONString(toDecimal512('123.456789', 6)) AS json_scale_6;
SELECT toJSONString(toDecimal512('123.456789', 3)) AS json_scale_3;
SELECT toJSONString(toDecimal512('123.456789', 0)) AS json_scale_0;

-- Test with very large numbers
SELECT '=== Very large numbers serialization ===';
SELECT toJSONString(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS json_large;
SELECT formatRow('CSV', toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS csv_large;

-- Test with arrays
SELECT '=== Array serialization ===';
SELECT toJSONString([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS json_array;
SELECT formatRow('CSV', [toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS csv_array;

-- Test with tuples
SELECT '=== Tuple serialization ===';
SELECT toJSONString((toDecimal512('123.456', 3), toDecimal512('789.123', 3))) AS json_tuple;
SELECT formatRow('CSV', (toDecimal512('123.456', 3), toDecimal512('789.123', 3))) AS csv_tuple;

-- Test with nullable values
SELECT '=== Nullable serialization ===';
SELECT toJSONString(toNullable(toDecimal512('123.456', 3))) AS json_nullable;
SELECT toJSONString(toNullable(toDecimal512('123.456', 3))) AS json_nullable_not_null;
SELECT toJSONString(toNullable(toDecimal512('123.456', 3))) AS json_nullable_null;

-- Test with different formats
SELECT '=== Different formats ===';
SELECT formatRow('JSONEachRow', toDecimal512('123.456', 3)) AS json_each_row;
SELECT formatRow('JSONCompactEachRow', toDecimal512('123.456', 3)) AS json_compact_each_row;
SELECT formatRow('JSONCompactEachRowWithNames', toDecimal512('123.456', 3)) AS json_compact_each_row_with_names;

