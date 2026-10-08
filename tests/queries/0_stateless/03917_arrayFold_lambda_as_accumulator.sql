-- Regression test: arrayFold should not cause a logical error when a lambda is passed as the accumulator (initial value).
-- Previously this caused LOGICAL_ERROR "Function 'modulo' argument is not resolved" in debug builds.
SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3], (a, b) -> a + b); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold((acc, x) -> acc + x, range(number), ((acc, x) -> if(x % 2, arrayPushFront(acc, x), arrayPushBack(acc, x)))) FROM system.numbers LIMIT 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
