SET input_format_binary_decode_types_in_binary_format = 0;
SET input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1;

SELECT * FROM format(Native, 'x IntervalSecond', x'01010178220b2a00000000000000'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(Native, 'x IntervalSecond', x'01010178227f2a00000000000000'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(Native, 'x IntervalSecond', x'0101017822ff2a00000000000000'); -- { serverError INCORRECT_DATA }

-- The endpoints of the valid kind range must still decode.
SELECT toTypeName(x), x FROM format(Native, 'x IntervalNanosecond', x'0101017822002a00000000000000');
SELECT toTypeName(x), x FROM format(Native, 'x IntervalYear', x'01010178220a2a00000000000000');

SET input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0;
SET input_format_binary_decode_types_in_binary_format = 1;

SELECT * FROM format(RowBinaryWithNamesAndTypes, 'x IntervalSecond', x'010178220b2a00000000000000'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(RowBinaryWithNamesAndTypes, 'x IntervalSecond', x'010178227f2a00000000000000'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(RowBinaryWithNamesAndTypes, 'x IntervalSecond', x'01017822ff2a00000000000000'); -- { serverError INCORRECT_DATA }

SELECT toTypeName(x), x FROM format(RowBinaryWithNamesAndTypes, 'x IntervalNanosecond', x'01017822002a00000000000000');
SELECT toTypeName(x), x FROM format(RowBinaryWithNamesAndTypes, 'x IntervalYear', x'010178220a2a00000000000000');
