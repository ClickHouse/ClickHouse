-- Test parameter validation for random distribution functions.
-- These would previously hang with extreme parameters.

-- randBinomial: trials too large
SELECT randBinomial(10000000000, 0.5); -- { serverError BAD_ARGUMENTS }

-- randNegativeBinomial: trials too large
SELECT randNegativeBinomial(10000000000, 0.5); -- { serverError BAD_ARGUMENTS }

-- randPoisson: mean too large
SELECT randPoisson(10000000000); -- { serverError BAD_ARGUMENTS }

-- randChiSquared: degrees of freedom too large
SELECT randChiSquared(1e7); -- { serverError BAD_ARGUMENTS }

-- randStudentT: degrees of freedom too large
SELECT randStudentT(1e7); -- { serverError BAD_ARGUMENTS }

-- randFisherF: degrees of freedom too large
SELECT randFisherF(1e7, 1); -- { serverError BAD_ARGUMENTS }
SELECT randFisherF(1, 1e7); -- { serverError BAD_ARGUMENTS }

-- randExponential: lambda must be positive
SELECT randExponential(toFloat64(0)); -- { serverError BAD_ARGUMENTS }
SELECT randExponential(toFloat64(-1)); -- { serverError BAD_ARGUMENTS }

-- randUniform: min > max
SELECT randUniform(toFloat64(10), toFloat64(5)); -- { serverError BAD_ARGUMENTS }

-- randNormal: negative stddev
SELECT randNormal(toFloat64(0), toFloat64(-1)); -- { serverError BAD_ARGUMENTS }

-- randLogNormal: negative stddev
SELECT randLogNormal(toFloat64(0), toFloat64(-1)); -- { serverError BAD_ARGUMENTS }

-- Valid calls still work (just check they don't throw; ignore the random output)
SELECT count() FROM (SELECT randBinomial(100, 0.5) FROM numbers(10));
SELECT count() FROM (SELECT randChiSquared(10) FROM numbers(10));
SELECT count() FROM (SELECT randPoisson(10) FROM numbers(10));
SELECT count() FROM (SELECT randUniform(0, 1) FROM numbers(10));
SELECT count() FROM (SELECT randNormal(0, 1) FROM numbers(10));
SELECT count() FROM (SELECT randExponential(1) FROM numbers(10));
