-- randBinomial rejects a probability outside [0, 1] before constructing std::binomial_distribution
SELECT randBinomial(10, 1.5); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(10, -0.5); -- { serverError BAD_ARGUMENTS }
SELECT randBinomial(10, 2); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM (SELECT randBinomial(10, 0.5) FROM numbers(1));
