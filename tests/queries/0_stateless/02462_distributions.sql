# Values should be between 0 and 1
SELECT DISTINCT if (a >= toFloat64(0) AND a <= toFloat64(1), 'Ok', 'Fail') FROM (SELECT randUniform(0, 1) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m >= toFloat64(-0.2) AND m <= toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT randNormal(0, 5) AS a FROM numbers(100000)));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randLogNormal(0, 5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randExponential(15) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randChiSquared(3) AS a FROM numbers(100000));
# Mean should be around 0
SELECT DISTINCT if (m > toFloat64(-0.2) AND m < toFloat64(0.2), 'Ok', 'Fail') FROM (SELECT avg(a) as m FROM (SELECT randStudentT(5) AS a FROM numbers(100000)));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randFisherF(3, 4) AS a FROM numbers(100000));
# There should be only 0s and 1s
SELECT a FROM (SELECT DISTINCT randBernoulli(0.5) AS a FROM numbers(100000)) ORDER BY a;
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randBinomial(3, 0.5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randNegativeBinomial(3, 0.5) AS a FROM numbers(100000));
# Values should be >= 0
SELECT DISTINCT if (a >= toFloat64(0), 'Ok', 'Fail') FROM (SELECT randPoisson(44) AS a FROM numbers(100000));
# No errors
SELECT randUniform(1, 2, 1), randNormal(0, 1, 'abacaba'), randLogNormal(0, 10, 'b'), randChiSquared(1, 1), randStudentT(7, '8'), randFisherF(23, 42, 100), randBernoulli(0.5, 2), randBinomial(3, 0.5, 1), randNegativeBinomial(3, 0.5, 2), randPoisson(44, 44) FORMAT Null;
