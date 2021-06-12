SELECT arrayFold([]); -- { serverError 42 }
SELECT arrayFold([1,2,3]); -- { serverError 42 }
SELECT arrayFold([1,2,3], [4,5,6]); -- { serverError 43 }
SELECT arrayFold(1234); -- { serverError 42 }
SELECT arrayFold(x, acc -> acc + x, 10, 20); -- { serverError 43 }
SELECT arrayFold(x, acc -> acc + x, 10, [20, 30, 40]); -- { serverError 43 }
SELECT arrayFold(x -> x * 2, [1,2,3,4], toInt64(3)); -- { serverError 43 }
SELECT arrayFold(x,acc -> acc+x, number, toInt64(0)) FROM system.numbers LIMIT 10; -- { serverError 43 }
SELECT arrayFold(x,y,acc -> acc + x * 2 + y * 3, [1,2,3,4], [5,6,7], toInt64(3)); -- { serverError 190 }
SELECT arrayFold(x,acc -> acc + x * 2 + y * 3, [1,2,3,4], [5,6,7,8], toInt64(3)); -- { serverError 47 }
SELECT arrayFold(x,acc -> acc + x * 2, [1,2,3,4], [5,6,7,8], toInt64(3)); -- { serverError 43 }
SELECT arrayFold(x,acc -> concat(acc,', ', x), [1, 2, 3, 4], '0') -- { serverError 44 }
