DROP TABLE IF EXISTS decimal;

CREATE TABLE decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8)
) ENGINE = Memory;

INSERT INTO decimal (a, b, c)
SELECT toDecimal32(number - 50, 4), toDecimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5
FROM system.numbers LIMIT 101;

SELECT count(a), count(b), count(c) FROM decimal;
SELECT [min(a), max(a)], [min(b), max(b)], [min(c), max(c)] FROM decimal;

SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal WHERE a > 0;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal WHERE a < 0;
SELECT sum(a+1), sum(b+1), sum(c+1), sumWithOverflow(a+1), sumWithOverflow(b+1), sumWithOverflow(c+1) FROM decimal;
SELECT sum(a-1), sum(b-1), sum(c-1), sumWithOverflow(a-1), sumWithOverflow(b-1), sumWithOverflow(c-1) FROM decimal;

SELECT (uniq(a), uniq(b), uniq(c)),
    (uniqCombined(a), uniqCombined(b), uniqCombined(c)),
    (uniqCombined(17)(a), uniqCombined(17)(b), uniqCombined(17)(c)),
    (uniqExact(a), uniqExact(b), uniqExact(c)),
    (102 - uniqHLL12(a) >= 0, 102 - uniqHLL12(b) >= 0, 102 - uniqHLL12(c) >= 0, uniqHLL12(a) - 99 >= 0, uniqHLL12(b) - 99 >= 0, uniqHLL12(c) - 99 >= 0)
FROM (SELECT * FROM decimal ORDER BY a);

SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM decimal WHERE a >= 0 AND a < 5;
SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM decimal WHERE a >= 0 AND a < 10;

SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM decimal;
SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM decimal WHERE a > 0;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM decimal;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM decimal WHERE a < 0;

SELECT median(a), median(b), median(c) as x, toTypeName(x) FROM decimal;
SELECT quantile(a), quantile(b), quantile(c) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantile(0.0)(a), quantile(0.0)(b), quantile(0.0)(c) FROM decimal WHERE a >= 0;
SELECT quantile(0.2)(a), quantile(0.2)(b), quantile(0.2)(c) FROM decimal WHERE a >= 0;
SELECT quantile(0.4)(a), quantile(0.4)(b), quantile(0.4)(c) FROM decimal WHERE a >= 0;
SELECT quantile(0.6)(a), quantile(0.6)(b), quantile(0.6)(c) FROM decimal WHERE a >= 0;
SELECT quantile(0.8)(a), quantile(0.8)(b), quantile(0.8)(c) FROM decimal WHERE a >= 0;
SELECT quantile(1.0)(a), quantile(1.0)(b), quantile(1.0)(c) FROM decimal WHERE a >= 0;
SELECT quantiles(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a) FROM decimal;
SELECT quantiles(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b) FROM decimal;
SELECT quantiles(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c) FROM decimal;

SELECT medianExact(a), medianExact(b), medianExact(c) as x, toTypeName(x) FROM decimal;
SELECT quantileExact(a), quantileExact(b), quantileExact(c) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExact(0.0)(a), quantileExact(0.0)(b), quantileExact(0.0)(c) FROM decimal WHERE a >= 0;
SELECT quantileExact(0.2)(a), quantileExact(0.2)(b), quantileExact(0.2)(c) FROM decimal WHERE a >= 0;
SELECT quantileExact(0.4)(a), quantileExact(0.4)(b), quantileExact(0.4)(c) FROM decimal WHERE a >= 0;
SELECT quantileExact(0.6)(a), quantileExact(0.6)(b), quantileExact(0.6)(c) FROM decimal WHERE a >= 0;
SELECT quantileExact(0.8)(a), quantileExact(0.8)(b), quantileExact(0.8)(c) FROM decimal WHERE a >= 0;
SELECT quantileExact(1.0)(a), quantileExact(1.0)(b), quantileExact(1.0)(c) FROM decimal WHERE a >= 0;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a) FROM decimal;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b) FROM decimal;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c) FROM decimal;

SELECT medianExactLow(a), medianExactLow(b), medianExactLow(c) as x, toTypeName(x) FROM decimal;
SELECT quantileExactLow(a), quantileExactLow(b), quantileExactLow(c) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExactLow(0.0)(a), quantileExactLow(0.0)(b), quantileExactLow(0.0)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactLow(0.2)(a), quantileExactLow(0.2)(b), quantileExactLow(0.2)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactLow(0.4)(a), quantileExactLow(0.4)(b), quantileExactLow(0.4)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactLow(0.6)(a), quantileExactLow(0.6)(b), quantileExactLow(0.6)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactLow(0.8)(a), quantileExactLow(0.8)(b), quantileExactLow(0.8)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactLow(1.0)(a), quantileExactLow(1.0)(b), quantileExactLow(1.0)(c) FROM decimal WHERE a >= 0;
SELECT quantilesExactLow(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a) FROM decimal;
SELECT quantilesExactLow(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b) FROM decimal;
SELECT quantilesExactLow(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c) FROM decimal;

SELECT medianExactHigh(a), medianExactHigh(b), medianExactHigh(c) as x, toTypeName(x) FROM decimal;
SELECT quantileExactHigh(a), quantileExactHigh(b), quantileExactHigh(c) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExactHigh(0.0)(a), quantileExactHigh(0.0)(b), quantileExactHigh(0.0)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactHigh(0.2)(a), quantileExactHigh(0.2)(b), quantileExactHigh(0.2)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactHigh(0.4)(a), quantileExactHigh(0.4)(b), quantileExactHigh(0.4)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactHigh(0.6)(a), quantileExactHigh(0.6)(b), quantileExactHigh(0.6)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactHigh(0.8)(a), quantileExactHigh(0.8)(b), quantileExactHigh(0.8)(c) FROM decimal WHERE a >= 0;
SELECT quantileExactHigh(1.0)(a), quantileExactHigh(1.0)(b), quantileExactHigh(1.0)(c) FROM decimal WHERE a >= 0;
SELECT quantilesExactHigh(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a) FROM decimal;
SELECT quantilesExactHigh(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b) FROM decimal;
SELECT quantilesExactHigh(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c) FROM decimal;

SELECT medianExactWeighted(a, 1), medianExactWeighted(b, 2), medianExactWeighted(c, 3) as x, toTypeName(x) FROM decimal;
SELECT quantileExactWeighted(a, 1), quantileExactWeighted(b, 2), quantileExactWeighted(c, 3) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExactWeighted(0.0)(a, 1), quantileExactWeighted(0.0)(b, 2), quantileExactWeighted(0.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.2)(a, 1), quantileExactWeighted(0.2)(b, 2), quantileExactWeighted(0.2)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.4)(a, 1), quantileExactWeighted(0.4)(b, 2), quantileExactWeighted(0.4)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.6)(a, 1), quantileExactWeighted(0.6)(b, 2), quantileExactWeighted(0.6)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.8)(a, 1), quantileExactWeighted(0.8)(b, 2), quantileExactWeighted(0.8)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(1.0)(a, 1), quantileExactWeighted(1.0)(b, 2), quantileExactWeighted(1.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a, 1) FROM decimal;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b, 2) FROM decimal;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c, 3) FROM decimal;

SELECT varPop(a) AS va, varPop(b) AS vb, varPop(c) AS vc, toTypeName(va), toTypeName(vb), toTypeName(vc) FROM decimal;
SELECT varPop(toFloat64(a)), varPop(toFloat64(b)), varPop(toFloat64(c)) FROM decimal;
SELECT varSamp(a) AS va, varSamp(b) AS vb, varSamp(c) AS vc, toTypeName(va), toTypeName(vb), toTypeName(vc) FROM decimal;
SELECT varSamp(toFloat64(a)), varSamp(toFloat64(b)), varSamp(toFloat64(c)) FROM decimal;

SELECT stddevPop(a) AS da, stddevPop(b) AS db, stddevPop(c) AS dc, toTypeName(da), toTypeName(db), toTypeName(dc) FROM decimal;
SELECT stddevPop(toFloat64(a)), stddevPop(toFloat64(b)), stddevPop(toFloat64(c)) FROM decimal;
SELECT stddevSamp(a) AS da, stddevSamp(b) AS db, stddevSamp(c) AS dc, toTypeName(da), toTypeName(db), toTypeName(dc) FROM decimal;
SELECT stddevSamp(toFloat64(a)), stddevSamp(toFloat64(b)), stddevSamp(toFloat64(c)) FROM decimal;

SELECT covarPop(a, a), covarPop(b, b), covarPop(c, c) FROM decimal; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT covarSamp(a, a), covarSamp(b, b), covarSamp(c, c) FROM decimal; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT corr(a, a), corr(b, b), corr(c, c) FROM decimal; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 LIMIT 0;

DROP TABLE decimal;

-- TODO: sumMap
-- TODO: other quantile(s)
-- TODO: groupArray, groupArrayInsertAt, groupUniqArray
-- TODO: topK
