-- A NaN or an infinity in the data makes the variance, the standard deviation and the test p-values NaN, not 0.
-- Finite data is unchanged, including the trim of a slightly negative variance to 0 for constant data.
-- https://github.com/ClickHouse/ClickHouse/issues/123855

SELECT 'variance nan', varPop(x), varSamp(x), stddevPop(x), stddevSamp(x) FROM VALUES('x Float64', (1), (nan), (3));
SELECT 'variance inf', varPop(x), varSamp(x), stddevPop(x), stddevSamp(x) FROM VALUES('x Float64', (1), (inf), (3));
SELECT 'variance constant', varPop(x), varSamp(x), stddevPop(x), stddevSamp(x) FROM VALUES('x Float64', (5), (5), (5));
SELECT 'variance finite', varPop(x), varSamp(x), round(stddevPop(x), 6), round(stddevSamp(x), 6) FROM VALUES('x Float64', (1), (2), (4));
SELECT 'regr_syy = regr_count * varPop', regr_syy(y, x), regr_count(y, x) * varPop(y) FROM VALUES('x Float64, y Float64', (1, nan), (2, 2), (3, 3));
SELECT 'two-sample t-test', studentTTest(x, g), welchTTest(x, g) FROM VALUES('x Float64, g UInt8', (1, 0), (nan, 0), (3, 0), (4, 1), (5, 1), (6, 1));
SELECT 'one-sample t-test', studentTTestOneSample(x, 2.) FROM VALUES('x Float64', (1), (nan), (3), (4));
SELECT 'z-test p-value', meanZTest(1., 1., 0.95)(x, g).2 FROM VALUES('x Float64, g UInt8', (1, 0), (nan, 0), (3, 0), (4, 1), (5, 1), (6, 1));
SELECT 't-test finite', round(studentTTest(x, g).2, 6) FROM VALUES('x Float64, g UInt8', (1, 0), (2, 0), (3, 0), (4, 1), (5, 1), (6, 1));
SELECT 'skew/kurt nan', skewPop(x), skewSamp(x), kurtPop(x), kurtSamp(x) FROM VALUES('x Float64', (1), (nan), (3));
SELECT 'skew/kurt constant', skewPop(x), kurtPop(x) FROM VALUES('x Float64', (5), (5), (5));
SELECT 'Float32 nan', varPop(x), stddevSamp(x) FROM VALUES('x Float32', (1), (nan), (3));
SELECT 'window', groupArray(v) FROM (SELECT varPop(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM VALUES('i UInt8, x Float64', (1, 1), (2, 3), (3, nan)));
