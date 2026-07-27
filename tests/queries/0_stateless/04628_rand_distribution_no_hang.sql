-- Extreme distribution parameters used to make the standard library sampler spin forever inside its
-- rejection loop, or walk over as many values as there are trials. Such a query could not be stopped by
-- `max_execution_time` or `KILL QUERY`, and constant folding ran into it already during query analysis.
-- `max_rand_distribution_parameter` and `max_rand_distribution_trials` bound the computation time and
-- are disabled by 0, but parameters outside the domain where the samplers are usable at all are always
-- rejected.

SET max_rand_distribution_parameter = 0;
SET max_rand_distribution_trials = 0;

SELECT randChiSquared(1.7976931348623157e308); -- { serverError BAD_ARGUMENTS }
SELECT randChiSquared(3e307); -- { serverError BAD_ARGUMENTS }
SELECT randStudentT(1.7976931348623157e308); -- { serverError BAD_ARGUMENTS }
SELECT randStudentT(3e307); -- { serverError BAD_ARGUMENTS }
SELECT randFisherF(1.7976931348623157e308, 1); -- { serverError BAD_ARGUMENTS }
SELECT randFisherF(1, 1.7976931348623157e308); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(18446744073709551615, 0.5); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(30000000000000, 0.999); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(1000000001, 0.5); -- { serverError BAD_ARGUMENTS }

-- Parameters inside the supported domain still work with the limits switched off.
SELECT count() FROM (SELECT randChiSquared(1e307) FROM numbers(1));
SELECT count() FROM (SELECT randStudentT(1e307) FROM numbers(1));
SELECT count() FROM (SELECT randFisherF(1e307, 1e307) FROM numbers(1));
SELECT count() FROM (SELECT randBinomial(1000000000, 0.999) FROM numbers(1));

-- The tunable limits still take precedence while they are below the hard bound.
SET max_rand_distribution_parameter = 50;
SET max_rand_distribution_trials = 100;
SELECT randChiSquared(51); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(101, 0.5); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM (SELECT randChiSquared(49) FROM numbers(1));
SELECT count() FROM (SELECT randBinomial(99, 0.5) FROM numbers(1));
