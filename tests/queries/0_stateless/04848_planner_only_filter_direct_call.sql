-- __plannerOnlyFilter should not be called directly, only for internal use.
-- Check that direct calls have defined behavior: a non-UInt8 argument is rejected, everything else evaluates to constant true at execution.

SELECT __plannerOnlyFilter('x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __plannerOnlyFilter(NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __plannerOnlyFilter(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __plannerOnlyFilter(0, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __plannerOnlyFilter(0);
SELECT __plannerOnlyFilter(1);
SELECT __plannerOnlyFilter(materialize(0));
SELECT __plannerOnlyFilter(number % 2) FROM numbers(3);
SELECT number FROM numbers(3) WHERE __plannerOnlyFilter(number % 2);
