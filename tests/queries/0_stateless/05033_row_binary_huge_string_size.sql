-- The size of a string in the binary formats is read from the data, and adding it to the current
-- size of the column must not overflow.

SELECT * FROM format(RowBinary, 's String', unhex('FFFFFFFFFFFFFFFFFF01'))
SETTINGS format_binary_max_string_size = 0; -- { serverError TOO_LARGE_STRING_SIZE }

SELECT * FROM format(RowBinary, 's String', unhex('FFFFFFFFFFFFFFFFFF01')); -- { serverError TOO_LARGE_STRING_SIZE }

SELECT * FROM format(RowBinary, 's String', unhex('0568656C6C6F'));

-- The same limit has to be enforced on the path that deserializes a string into a `Field`, which is
-- how the values of the paths that go into the shared data of a `JSON` column are read.

SELECT * FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('01016115FFFFFFFFFFFFFFFFFF01'))
SETTINGS format_binary_max_string_size = 0; -- { serverError TOO_LARGE_STRING_SIZE }

SELECT * FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('0101611503616263'));

-- `Dynamic` and `JSON` values nest into each other, and the type of every nested value comes from
-- the data, so the depth of the recursion of the binary deserialization is bounded only by the size
-- of the input, on the column path as well as on the `Field` path.

SELECT * FROM format(RowBinary, 'j JSON', unhex(repeat('01016130001010000000', 10000) || '00')); -- { serverError TOO_DEEP_RECURSION }
