-- The size of a string in the binary formats is read from the data, and adding it to the current
-- size of the column must not overflow.

SELECT * FROM format(RowBinary, 's String', unhex('FFFFFFFFFFFFFFFFFF01'))
SETTINGS format_binary_max_string_size = 0; -- { serverError TOO_LARGE_STRING_SIZE }

SELECT * FROM format(RowBinary, 's String', unhex('FFFFFFFFFFFFFFFFFF01')); -- { serverError TOO_LARGE_STRING_SIZE }

SELECT * FROM format(RowBinary, 's String', unhex('0568656C6C6F'));
