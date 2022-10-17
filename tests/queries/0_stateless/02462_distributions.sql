# Values should be between 0 and 1
SELECT DISTINCT if (a >= toFloat64(0) AND a <= toFloat64(1), 'Ok', 'Fail') FROM (SELECT uniformDistribution(0, 1) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m >= toFloat64(-0.2) AND m <= toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT normalDistribution(0, 5) AS a FROM numbers(100000)));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT logNormalDistribution(0, 5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT exponentialDistribution(15) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT chiSquaredDistribution(3) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m > toFloat64(-0.2) AND m < toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT studentTDistribution(5) AS a FROM numbers(100000)));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT fisherFDistribution(3, 4) AS a FROM numbers(100000));
# There should be only 0s and 1s
SELECT a FROM (SELECT DISTINCT bernoulliDistribution(0.5) AS a FROM numbers(100000)) ORDER BY a;
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT binomialDistribution(3, 0.5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT negativeBinomialDistribution(3, 0.5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT poissonDistribution(44) AS a FROM numbers(100000));
