SET optimize_arithmetic_operations_in_aggregate_functions = 1;

SELECT max(multiply(1));  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT min(multiply(2));-- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sum(multiply(3));  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT max(plus(1));  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT min(plus(2)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sum(plus(3));  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT max(multiply());  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT min(multiply(1, 2 ,3)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sum(plus() + multiply());  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT sum(plus(multiply(42, 3), multiply(42)));  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
