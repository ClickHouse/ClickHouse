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

-- A typed path of a `JSON` type recurses back into the deserialization of `JSON` directly, without
-- passing through `Dynamic`, and the depth of such a type also comes from the data (the binary
-- encoding of a `Dynamic` value starts with the encoding of its type), so the recursion has to be
-- checked on that path as well.

SELECT * FROM format(RowBinary, 'd Dynamic', unhex('30000000010161' || '30000000000000' || '0000' || '010161' || '00'))
SETTINGS input_format_binary_max_type_complexity = 0;

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(repeat('30000000010161', 10000) || '30000000000000' || repeat('0000', 10000) || repeat('010161', 10000) || '00'))
SETTINGS input_format_binary_max_type_complexity = 0; -- { serverError TOO_DEEP_RECURSION }

-- The type of a `Dynamic` value comes from the data as well when it is not `Dynamic` or `JSON`, and it can
-- nest arbitrarily deep through the ordinary containers, whose deserialization then recurses through their
-- serializations without passing through `Dynamic` or `JSON` again.

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(repeat('1E', 3) || '01' || repeat('01', 3) || '00'))
SETTINGS input_format_binary_max_type_complexity = 0;

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(repeat('1E', 100000) || '01' || repeat('01', 100000) || '00'))
SETTINGS input_format_binary_max_type_complexity = 0; -- { serverError TOO_DEEP_RECURSION }

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(repeat('1F01', 3) || '01' || '00'))
SETTINGS input_format_binary_max_type_complexity = 0;

SELECT * FROM format(RowBinary, 'd Dynamic', unhex(repeat('1F01', 100000) || '01' || '00'))
SETTINGS input_format_binary_max_type_complexity = 0; -- { serverError TOO_DEEP_RECURSION }
