-- `randBinomial` is bounded by a hard limit on the number of trials, because the sampler of the standard
-- library may walk over as many values as there are trials. For the degenerate probabilities 0 and 1 it
-- returns 0 and the number of trials without entering that walk, so those two cases stay available for any
-- number of trials even though the tunable limit is switched off.

SET max_rand_distribution_trials = 0;

SELECT randBinomial(18446744073709551615, 0);
SELECT randBinomial(18446744073709551615, 1);
SELECT randBinomial(30000000000000, 0);
SELECT randBinomial(30000000000000, 1);
SELECT randBinomial(1000000001, 0);
SELECT randBinomial(1000000001, 1);

-- Everything strictly between the endpoints is still rejected above the hard limit.
SELECT randBinomial(1000000001, 0.5); -- { serverError BAD_ARGUMENTS }

-- The tunable limit still applies to the degenerate cases as well.
SET max_rand_distribution_trials = 100;
SELECT randBinomial(101, 0); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(101, 1); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(100, 0);
SELECT randBinomial(100, 1);
