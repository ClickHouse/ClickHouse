SET optimize_arithmetic_operations_in_aggregate_functions = 1;

SELECT max(multiply(1));  -- { serverError 42 }
SELECT min(multiply(2));-- { serverError 42 }
SELECT sum(multiply(3));  -- { serverError 42 }

SELECT max(plus(1));  -- { serverError 42 }
SELECT min(plus(2)); -- { serverError 42 }
SELECT sum(plus(3));  -- { serverError 42 }

SELECT max(multiply());  -- { serverError 42 }
SELECT min(multiply(1, 2 ,3)); -- { serverError 42 }
SELECT sum(plus() + multiply());  -- { serverError 42 }

SELECT sum(plus(multiply(42, 3), multiply(42)));  -- { serverError 42 }
