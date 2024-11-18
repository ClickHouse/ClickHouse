-- type correctness tests
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8))), 10);
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8))), 10);
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1)))), 10);
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1)))), 10);
select floor(arrayPrAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1]), 10);
select floor(arrayPrAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1]), 10);

-- output value correctness test
select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
select floor(arrayPrAUC([0.1, 0.4, 0.4, 0.35, 0.8], [0, 0, 1, 1, 1]), 10);
select floor(arrayPrAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0]), 10);
select floor(arrayPrAUC([0.1, 0.35, 0.4, 0.4, 0.8], [1, 0, 1, 0, 0]), 10);
select floor(arrayPrAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0]), 10);
select floor(arrayPrAUC([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);
select floor(arrayPrAUC([0, 1, 1, 2, 2, 2, 3, 3, 3, 3], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);
-- edge cases
SELECT floor(arrayPrAUC([1], [1]), 10);
SELECT floor(arrayPrAUC([1], [0]), 10);
SELECT floor(arrayPrAUC([0], [0]), 10);
SELECT floor(arrayPrAUC([0], [1]), 10);
SELECT floor(arrayPrAUC([1, 1], [1, 1]), 10);
SELECT floor(arrayPrAUC([1, 1], [0, 0]), 10);
SELECT floor(arrayPrAUC([1, 1], [0, 1]), 10);
SELECT floor(arrayPrAUC([0, 1], [0, 1]), 10);
SELECT floor(arrayPrAUC([1, 0], [0, 1]), 10);
SELECT floor(arrayPrAUC([0, 0, 1], [0, 1, 1]), 10);
SELECT floor(arrayPrAUC([0, 1, 1], [0, 1, 1]), 10);
SELECT floor(arrayPrAUC([0, 1, 1], [0, 0, 1]), 10);

-- negative tests
select arrayPrAUC([], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayPrAUC([0, 0, 1, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayPrAUC([0.1, 0.35], [0, 0, 1, 1]); -- { serverError BAD_ARGUMENTS }
select arrayPrAUC([0.1, 0.4, 0.35, 0.8], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [1, 1, 0, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }