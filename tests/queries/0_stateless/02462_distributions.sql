# Values should be between 0 and 1
SELECT DISTINCT if (a >= 0 AND a <= 1, 'Ok', 'Fail') FROM (SELECT uniformDistribution(0, 1) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m > toFloat64(-0.2) AND m < toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT normalDistribution(0, 5) AS a FROM numbers(100000)));
# Values should be > 0
SELECT DISTINCT if (a > 0, 'Ok', 'Fail') FROM (SELECT logNormalDistribution(0, 5) AS a FROM numbers(100000));
# Values should be > 0
SELECT DISTINCT if (a > 0, 'Ok', 'Fail') FROM (SELECT chiSquaredDistribution(3) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m > toFloat64(-0.2) AND m < toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT studentTDistribution(5) AS a FROM numbers(100000)));
# Values should be > 0
SELECT DISTINCT if (a > 0, 'Ok', 'Fail') FROM (SELECT fisherFDistribution(3) AS a FROM numbers(100000));
