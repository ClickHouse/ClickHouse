-- Test configurable random distribution limits via settings.

-- With default settings, large values are rejected
SELECT randBinomial(10000000000, 0.5); -- { serverError BAD_ARGUMENTS }
SELECT randChiSquared(1e7); -- { serverError BAD_ARGUMENTS }

-- Setting value 0 disables the limit
SET max_rand_distribution_trials = 0;
SET max_rand_distribution_parameter = 0;
SELECT count() FROM (SELECT randChiSquared(1e7) FROM numbers(1));

-- Custom limits: lower the threshold
SET max_rand_distribution_trials = 100;
SET max_rand_distribution_parameter = 50;

-- Values above limit are rejected
SELECT randBinomial(101, 0.5); -- { serverError BAD_ARGUMENTS }
SELECT randNegativeBinomial(101, 0.5); -- { serverError BAD_ARGUMENTS }
SELECT randPoisson(101); -- { serverError BAD_ARGUMENTS }
SELECT randChiSquared(51); -- { serverError BAD_ARGUMENTS }
SELECT randStudentT(51); -- { serverError BAD_ARGUMENTS }
SELECT randFisherF(51, 1); -- { serverError BAD_ARGUMENTS }
SELECT randFisherF(1, 51); -- { serverError BAD_ARGUMENTS }

-- Values within limit work
SELECT count() FROM (SELECT randBinomial(99, 0.5) FROM numbers(1));
SELECT count() FROM (SELECT randChiSquared(49) FROM numbers(1));
