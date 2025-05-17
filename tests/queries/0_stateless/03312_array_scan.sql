SELECT '-- Negative tests';
SELECT arrayScan(); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayScan(1); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayScan(1, toUInt64(0)); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayScan(1, emptyArrayUInt64(), toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayScan( acc,x -> x,  emptyArrayString(), toInt8(0)); -- { serverError TYPE_MISMATCH }
SELECT arrayScan( acc,x -> x,  'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayScan( acc,x,y -> x,  [0, 1], 'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayScan( acc,x -> x,  [0, 1], [2, 3], toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayScan( acc,x,y -> x,  [0, 1], [2, 3, 4], toUInt8(0)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- Const arrays';
SELECT arrayScan( acc,x -> acc+x*2,  [1, 2, 3, 4], toInt64(3));
SELECT arrayScan( acc,x -> acc+x*2,  emptyArrayInt64(), toInt64(3));
SELECT arrayScan( acc,x,y -> acc+x*2+y*3,  [1, 2, 3, 4], [5, 6, 7, 8], toInt64(3));
SELECT arrayScan( acc,x -> arrayPushBack(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayScan( acc,x -> arrayPushFront(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayScan( acc,x -> (arrayPushFront(acc.1, x),arrayPushBack(acc.2, x)),  [1, 2, 3, 4], (emptyArrayInt64(), emptyArrayInt64()));
SELECT arrayScan( acc,x -> x%2 ? (arrayPushBack(acc.1, x), acc.2): (acc.1, arrayPushBack(acc.2, x)),  [1, 2, 3, 4, 5, 6], (emptyArrayInt64(), emptyArrayInt64()));

SELECT '-- Non-const arrays';
SELECT arrayScan( acc,x -> acc+x,  range(number), number) FROM system.numbers LIMIT 5;
SELECT arrayScan( acc,x -> arrayPushFront(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;
SELECT arrayScan( acc,x -> x%2 ? arrayPushFront(acc,x) : arrayPushBack(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;
