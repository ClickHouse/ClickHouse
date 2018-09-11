SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE test.decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8)
) ENGINE = Memory;

INSERT INTO test.decimal (a, b, c)
SELECT toDecimal32(number - 50, 4) / 2, toDecimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5
FROM system.numbers LIMIT 101;

SELECT count(a), count(b), count(c) FROM test.decimal;
SELECT min(a), min(b), min(c) FROM test.decimal;
SELECT max(a), max(b), max(c) FROM test.decimal;

SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM test.decimal;
SELECT sum(a+1), sum(b+1), sum(c+1), sumWithOverflow(a+1), sumWithOverflow(b+1), sumWithOverflow(c+1) FROM test.decimal;
SELECT sum(a-1), sum(b-1), sum(c-1), sumWithOverflow(a-1), sumWithOverflow(b-1), sumWithOverflow(c-1) FROM test.decimal;

--SELECT avg(a), avg(b), avg(c) FROM test.decimal;

SELECT argMin(a, b), argMin(b, a), argMin(c, a) FROM test.decimal;
SELECT argMax(a, b), argMax(b, a), argMax(c, a) FROM test.decimal;

SELECT uniq(a), uniq(b), uniq(c) FROM test.decimal;
SELECT uniqCombined(a), uniqCombined(b), uniqCombined(c) FROM test.decimal;
SELECT uniqExact(a), uniqExact(b), uniqExact(c) FROM test.decimal;
SELECT uniqHLL12(a), uniqHLL12(b), uniqHLL12(c) FROM test.decimal;

--SELECT median(a), median(b), median(c) FROM test.decimal;
--SELECT quantile(a), quantile(b), quantile(c) FROM test.decimal;
--SELECT quantile(0.9)(a), quantile(0.9)(b), quantile(0.9)(c) FROM test.decimal;
-- TODO: quantileDeterministic, quantileTiming
--SELECT quantileExact(a), quantileExact(b), quantileExact(c) FROM test.decimal;
--SELECT quantileExact(0.9)(a), quantileExact(0.9)(b), quantileExact(0.9)(c) FROM test.decimal;
--SELECT quantileTDigest(a), quantileTDigest(b), quantileTDigest(c) FROM test.decimal;
--SELECT quantileTDigest(0.9)(a), quantileTDigest(0.9)(b), quantileTDigest(0.9)(c) FROM test.decimal;
--SELECT quantiles(0.5, 0.9)(a), quantiles(0.5, 0.9)(b), quantiles(0.5, 0.9)(c) FROM test.decimal;

-- TODO: sumMap
-- TODO: groupArray, groupArrayInsertAt, groupUniqArray

--SELECT topK(2)(a), topK(2)(b), topK(2)(c) FROM test.decimal; TODO: deterministic
