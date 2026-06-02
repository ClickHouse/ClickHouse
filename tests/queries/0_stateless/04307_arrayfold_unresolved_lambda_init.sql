-- Regression test for STID 1719-3ae5:
-- Passing a lambda where a concrete value is expected (e.g. the initial accumulator
-- of `arrayFold`) used to crash the server with SIGSEGV in `removeNullable` called
-- from `FunctionArrayMapped::getReturnTypeImpl`. The unresolved-lambda placeholder
-- `DataTypeFunction` propagated into the synthetic outer lambda's argument types
-- via `getLambdaArgumentTypes`, then the inner function (e.g. `arrayExists`) accessed
-- the placeholder's null `return_type` without guarding.

-- arrayFold first arg = function reference (wrapped as synthetic lambda)
-- arrayFold third arg = a lambda used as initial accumulator -> rejected
SELECT arrayFold(arrayExists, range(number), ((acc, x) -> if(x % 2, arraySlice(x, acc), arrayPushBack(acc, x)))) FROM system.numbers LIMIT 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold(arrayCount,  range(number), ((acc, x) -> if(x % 2, arraySlice(x, acc), arrayPushBack(acc, x)))) FROM system.numbers LIMIT 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold(arrayFirst,  range(number), ((acc, x) -> if(x % 2, arraySlice(x, acc), arrayPushBack(acc, x)))) FROM system.numbers LIMIT 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same shape with a lambda CTE bound via WITH
WITH (a -> a + 1) AS f SELECT arrayFold(arrayExists, [[1, 2, 3]], f); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: legitimate arrayFold and higher-order calls still work
SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3], 0::UInt64);
SELECT arrayExists(x -> x > 1, [1, 2, 3]);
SELECT arrayCount(x -> x > 1, [1, 2, 3]);
SELECT arrayFirst(x -> x > 1, [1, 2, 3]);
