-- type correctness tests
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8))), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8))), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1)))), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1)))), 10);
select floor(arrayAUCPR(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1]), 10);
select floor(arrayAUCPR(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1]), 10);

-- output value correctness test
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.4, 0.35, 0.8], [0, 0, 1, 1, 1]), 10);
select floor(arrayAUCPR([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0]), 10);
select floor(arrayAUCPR([0.1, 0.35, 0.4, 0.4, 0.8], [1, 0, 1, 0, 0]), 10);
select floor(arrayAUCPR([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0]), 10);
select floor(arrayAUCPR([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);
select floor(arrayAUCPR([0, 1, 1, 2, 2, 2, 3, 3, 3, 3], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);

-- output shouldn't change when passing [0, 0, 0] to the offsets arg
select floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, 0, 0]), 10);
select floor(arrayAUCPR([0.1, 0.4, 0.4, 0.35, 0.8], [0, 0, 1, 1, 1], [0, 0, 0]), 10);
select floor(arrayAUCPR([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], [0, 0, 0]), 10);
select floor(arrayAUCPR([0.1, 0.35, 0.4, 0.4, 0.8], [1, 0, 1, 0, 0], [0, 0, 0]), 10);
select floor(arrayAUCPR([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], [0, 0, 0]), 10);
select floor(arrayAUCPR([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1], [0, 0, 0]), 10);
select floor(arrayAUCPR([0, 1, 1, 2, 2, 2, 3, 3, 3, 3], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1], [0, 0, 0]), 10);

-- edge cases
SELECT floor(arrayAUCPR([1], [1]), 10);
SELECT floor(arrayAUCPR([1], [0]), 10);
SELECT floor(arrayAUCPR([0], [0]), 10);
SELECT floor(arrayAUCPR([0], [1]), 10);
SELECT floor(arrayAUCPR([1, 1], [1, 1]), 10);
SELECT floor(arrayAUCPR([1, 1], [0, 0]), 10);
SELECT floor(arrayAUCPR([1, 1], [0, 1]), 10);
SELECT floor(arrayAUCPR([0, 1], [0, 1]), 10);
SELECT floor(arrayAUCPR([1, 0], [0, 1]), 10);
SELECT floor(arrayAUCPR([0, 0, 1], [0, 1, 1]), 10);
SELECT floor(arrayAUCPR([0, 1, 1], [0, 1, 1]), 10);
SELECT floor(arrayAUCPR([0, 1, 1], [0, 0, 1]), 10);

-- alias
SELECT floor(arrayPRAUC([1], [1]), 10);

-- general negative tests
select arrayAUCPR([], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0, 0, 1, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayAUCPR([0.1, 0.35], [0, 0, 1, 1]); -- { serverError BAD_ARGUMENTS }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, 0, 0], [1, 1, 0, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayAUCPR(['a', 'b', 'c', 'd'], [1, 0, 1, 1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0.1, 0.4, NULL, 0.8], [0, 0, 1, 1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, NULL, 1, 1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- negative tests for optional argument
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, 0, NULL]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], ['a', 'b', 'c']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, 1, 0, 0]); -- { serverError BAD_ARGUMENTS }
select arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, -1, 0]); -- { serverError BAD_ARGUMENTS }
select arrayAUCPR(x, y, z) from (
  select [1] as x, [0] as y, [0, 0, 0, 0, 0, 0] as z
  UNION ALL
  select [1] as x, [0] as y, [] as z
); -- { serverError BAD_ARGUMENTS }
select arrayAUCPR(x, y, z) from (
  select [1] as x, [0] as y, [0, 0] as z
  UNION ALL
  select [1] as x, [1] as y, [0, 0, 0, 0] as z
); -- { serverError BAD_ARGUMENTS }
