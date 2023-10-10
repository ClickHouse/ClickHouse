SELECT 'Negative tests';
SELECT arrayFold(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayFold(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayFold(1, toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( x,acc -> x,  emptyArrayString(), toInt8(0)); -- { serverError TYPE_MISMATCH }
SELECT arrayFold( x,acc -> x,  'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( x,y,acc -> x,  [0, 1], 'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( x,acc -> x,  [0, 1], [2, 3], toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( x,y,acc -> x,  [0, 1], [2, 3, 4], toUInt8(0)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT 'Const arrays';
SELECT arrayFold( x,acc -> acc+x*2,  [1, 2, 3, 4], toInt64(3));
SELECT arrayFold( x,acc -> acc+x*2,  emptyArrayInt64(), toInt64(3));
SELECT arrayFold( x,y,acc -> acc+x*2+y*3,  [1, 2, 3, 4], [5, 6, 7, 8], toInt64(3));
SELECT arrayFold( x,acc -> arrayPushBack(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayFold( x,acc -> arrayPushFront(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayFold( x,acc -> (arrayPushFront(acc.1, x),arrayPushBack(acc.2, x)),  [1, 2, 3, 4], (emptyArrayInt64(), emptyArrayInt64()));
SELECT arrayFold( x,acc -> x%2 ? (arrayPushBack(acc.1, x), acc.2): (acc.1, arrayPushBack(acc.2, x)),  [1, 2, 3, 4, 5, 6], (emptyArrayInt64(), emptyArrayInt64()));

SELECT 'Non-const arrays';
SELECT arrayFold( x,acc -> acc+x,  range(number), number) FROM system.numbers LIMIT 5;
SELECT arrayFold( x,acc -> arrayPushFront(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;
SELECT arrayFold( x,acc -> x%2 ? arrayPushFront(acc,x) : arrayPushBack(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;
