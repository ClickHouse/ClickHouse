SELECT toDecimal32('42.42', 4) AS x, toDecimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT toDecimal32('42.42', 4) AS x, toDecimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT toDecimal32('42.42', 4) AS x, toDecimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT toDecimal32('42.42', 4) AS x, toDecimal32(sqrt(x), 3) AS y, y * y;
SELECT toDecimal32('42.42', 4) AS x, toDecimal32(cbrt(x), 4) AS y, toDecimal64(y, 4) * y * y;
SELECT toDecimal32('1.0', 5) AS x, erf(x), erfc(x);
SELECT toDecimal32('42.42', 4) AS x, lgamma(x), tgamma(x);

SELECT toDecimal32('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal32(pi(), 8) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal32('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT toDecimal64('42.42', 4) AS x, toDecimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT toDecimal64('42.42', 4) AS x, toDecimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT toDecimal64('42.42', 4) AS x, toDecimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT toDecimal64('42.42', 4) AS x, toDecimal32(sqrt(x), 3) AS y, y * y;
SELECT toDecimal64('42.42', 4) AS x, toDecimal32(cbrt(x), 4) AS y, toDecimal64(y, 4) * y * y;
SELECT toDecimal64('1.0', 5) AS x, erf(x), erfc(x);
SELECT toDecimal64('42.42', 4) AS x, lgamma(x), tgamma(x);

SELECT toDecimal64('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal64(pi(), 17) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal64('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT toDecimal128('42.42', 4) AS x, toDecimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT toDecimal128('42.42', 4) AS x, toDecimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT toDecimal128('42.42', 4) AS x, toDecimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT toDecimal128('42.42', 4) AS x, toDecimal32(sqrt(x), 3) AS y, y * y;
SELECT toDecimal128('42.42', 4) AS x, toDecimal32(cbrt(x), 4) AS y, toDecimal64(y, 4) * y * y;
SELECT toDecimal128('1.0', 5) AS x, erf(x), erfc(x);
SELECT toDecimal128('42.42', 4) AS x, lgamma(x), tgamma(x);

SELECT toDecimal128('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal128(pi(), 37) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT toDecimal128('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT toDecimal32('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
SELECT toDecimal64('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
SELECT toDecimal128('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
