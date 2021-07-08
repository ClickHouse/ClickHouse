SELECT arrayFold(x,acc -> acc + x * 2, [1,2,3,4], toInt64(3));
SELECT arrayFold(x,acc -> acc + x * 2, emptyArrayInt64(), toInt64(3));
SELECT arrayFold(x,y,acc -> acc + x * 2 + y * 3, [1,2,3,4], [5,6,7,8], toInt64(3));
SELECT arrayFold(x,y,z,acc -> acc + x * 2 + y * 3 + z * 4, [1,2,3,4], [5,6,7,8], [9,10,11,12], toInt64(3));
SELECT arrayFold(x,acc -> arrayPushBack(acc,x),  [1,2,3,4], emptyArrayInt64());
SELECT arrayFold(x,acc -> arrayPushFront(acc,x), [1,2,3,4], emptyArrayInt64());
SELECT arrayFold(x,acc -> (arrayPushFront(acc.1,x), arrayPushBack(acc.2,x)), [1,2,3,4], (emptyArrayInt64(), emptyArrayInt64()));
SELECT arrayFold(x,acc -> x % 2 ? (arrayPushBack(acc.1,x), acc.2): (acc.1, arrayPushBack(acc.2,x)), [1,2,3,4,5,6], (emptyArrayInt64(), emptyArrayInt64()));
