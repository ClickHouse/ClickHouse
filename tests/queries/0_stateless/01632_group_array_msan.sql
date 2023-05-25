SELECT groupArrayMerge(1048577)(y * 1048576) FROM (SELECT groupArrayState(9223372036854775807)(x) AS y FROM (SELECT 1048576 AS x)) FORMAT Null; -- { serverError 43 }
SELECT groupArrayMerge(1048577)(y * 1048576) FROM (SELECT groupArrayState(1048577)(x) AS y FROM (SELECT 1048576 AS x)) FORMAT Null;
SELECT groupArrayMerge(9223372036854775807)(y * 1048576) FROM (SELECT groupArrayState(9223372036854775807)(x) AS y FROM (SELECT 1048576 AS x)) FORMAT Null;
SELECT quantileResampleMerge(0.5, 257, 65536, 1)(tuple(*).1) FROM (SELECT quantileResampleState(0.10, 1, 2, 42)(number, number) FROM numbers(100)); -- { serverError 43 }
