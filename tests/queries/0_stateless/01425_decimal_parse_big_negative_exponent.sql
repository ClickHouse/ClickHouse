SELECT '-1E9-1E9-1E9-1E9' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '-1E9' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '1E-9' AS x, toDecimal32(x, 0);
SELECT '1E-8' AS x, toDecimal32(x, 0);
SELECT '1E-7' AS x, toDecimal32(x, 0);
SELECT '1e-7' AS x, toDecimal32(x, 0);
SELECT '1E-9' AS x, toDecimal32(x, 9);
SELECT '1E-9' AS x, toDecimal32(x, 10); -- { serverError 69 }
SELECT '1E-10' AS x, toDecimal32(x, 10); -- { serverError 69 }
SELECT '1E-10' AS x, toDecimal32(x, 9);
