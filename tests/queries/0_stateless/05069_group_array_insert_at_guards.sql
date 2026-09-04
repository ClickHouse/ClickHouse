-- Coverage test for AggregateFunctionGroupArrayInsertAt error paths and branch guards.
-- Targets uncovered paths in src/AggregateFunctions/AggregateFunctionGroupArrayInsertAt.cpp:
--   lines 77-78:   too-many-parameters guard (> 2 params)
--   lines 85-87:   length_to_resize > MAX_SIZE guard
--   lines 91-92:   second argument not UInt guard
--   lines 121-124: position >= MAX_SIZE guard (runtime)
--   lines 128-131: duplicate position guard (else-if branch, no error)

-- lines 77-78: more than two parameters → TOO_MANY_ARGUMENTS_FOR_FUNCTION
SELECT groupArrayInsertAt(0, 10, 5)(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 0)); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

-- lines 85-87: length_to_resize (second param) exceeds 16777215 → TOO_LARGE_ARRAY_SIZE
SELECT groupArrayInsertAt(0, 16777216)(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 0)); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- lines 91-92: position argument has non-UInt type → ILLEGAL_TYPE_OF_ARGUMENT
SELECT groupArrayInsertAt(x, pos) FROM VALUES('x Int32, pos String', (1, 'a')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- lines 121-124: position value at runtime >= 16777215 → TOO_LARGE_ARRAY_SIZE
SELECT groupArrayInsertAt(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 16777215)); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- lines 128-131: duplicate position — second write is silently skipped (else-if !arr[position].isNull()).
-- First insertion ('a' at 0) wins; second ('b' at 0) is ignored; 'c' goes to position 1.
SELECT groupArrayInsertAt(val, pos) FROM VALUES('val String, pos UInt32', ('a', 0), ('b', 0), ('c', 1));
