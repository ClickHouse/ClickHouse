-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

-- The `row_size` prefix of a Row is a length prefix on untrusted binary input: an oversized one
-- must be rejected before the payload is allocated.
SELECT * FROM format(RowBinary, 'r Row(a UInt8, b UInt8)', '\xFF\xFF\xFF\xFF\x7F'); -- { serverError TOO_LARGE_STRING_SIZE }

-- The bound is `format_binary_max_string_size`, as for other length-prefixed binary values.
SELECT * FROM format(RowBinary, 'r Row(a UInt8, b UInt8)', '\x02\x01\x02') SETTINGS format_binary_max_string_size = 1; -- { serverError TOO_LARGE_STRING_SIZE }
SELECT * FROM format(RowBinary, 'r Row(a UInt8, b UInt8)', '\x02\x01\x02');
