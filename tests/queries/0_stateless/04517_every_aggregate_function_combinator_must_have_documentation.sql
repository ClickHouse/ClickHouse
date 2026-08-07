-- Every aggregate function combinator must carry a non-empty description in its structured documentation.
SELECT name FROM system.aggregate_function_combinators WHERE length(description) < 10;
