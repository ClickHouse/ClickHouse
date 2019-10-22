DROP TABLE IF EXISTS decimal;

CREATE TABLE decimal
(
    a Decimal(6, 4),
    b Decimal(16, 7),
    c Decimal(20, 8)
) ENGINE = Memory;

SELECT count(a), count(b), count(c) FROM decimal;
SELECT [min(a), max(a)], [min(b), max(b)], [min(c), max(c)] FROM decimal;

SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal WHERE a > 0;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM decimal WHERE a < 0;
SELECT sum(a+1), sum(b+1), sum(c+1), sumWithOverflow(a+1), sumWithOverflow(b+1), sumWithOverflow(c+1) FROM decimal;
SELECT sum(a-1), sum(b-1), sum(c-1), sumWithOverflow(a-1), sumWithOverflow(b-1), sumWithOverflow(c-1) FROM decimal;

SELECT avg(a) as aa, avg(b) as ab, avg(c) as ac, toTypeName(aa), toTypeName(ab),toTypeName(ac) FROM decimal;
SELECT avg(a) as aa, avg(b) as ab, avg(c) as ac, toTypeName(aa), toTypeName(ab),toTypeName(ac) FROM decimal WHERE a > 0;
SELECT avg(a) as aa, avg(b) as ab, avg(c) as ac, toTypeName(aa), toTypeName(ab),toTypeName(ac) FROM decimal WHERE a < 0;

SELECT (uniq(a), uniq(b), uniq(c)),
    (uniqCombined(a), uniqCombined(b), uniqCombined(c)),
    (uniqCombined(17)(a), uniqCombined(17)(b), uniqCombined(17)(c)),
    (uniqExact(a), uniqExact(b), uniqExact(c)),
    (uniqHLL12(a), uniqHLL12(b), uniqHLL12(c))
FROM (SELECT * FROM decimal ORDER BY a);

SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM decimal WHERE a >= 0 AND a < 5;
SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM decimal WHERE a >= 0 AND a < 10;

SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM decimal;
SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM decimal WHERE a > 0;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM decimal;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM decimal WHERE a < 0;

SELECT median(a) as ma, median(b) as mb, median(c) as mc, toTypeName(ma),toTypeName(mb),toTypeName(mc) FROM decimal;
SELECT quantile(a) as qa, quantile(b) as qb, quantile(c) as qc, toTypeName(qa),toTypeName(qb),toTypeName(qc) FROM decimal WHERE a < 0;
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

SELECT covarPop(a, a), covarPop(b, b), covarPop(c, c) FROM decimal; -- { serverError 43 }
SELECT covarSamp(a, a), covarSamp(b, b), covarSamp(c, c) FROM decimal; -- { serverError 43 }
SELECT corr(a, a), corr(b, b), corr(c, c) FROM decimal; -- { serverError 43 }
SELECT 1 LIMIT 0;

DROP TABLE decimal;

