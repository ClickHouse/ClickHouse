-- The function composition operator: `f | g` applies `f` and then `g`.

-- The composition is resolved only in the analyzer.
SET enable_analyzer = 1;

SELECT arrayMap(plus(_, 1) | multiply(_, 2), [1, 2, 3]);
SELECT arrayMap(multiply(_1, _2) | plus(_, 2), [1, 2, 3], [4, 5, 6]);
SELECT arrayMap((x -> x * 2) | toString(_), [1, 2, 3]);
SELECT arrayMap((x -> x * 2) | (x -> x * 2), [1, 2, 3]);

-- The operator is left-associative and can be chained.
SELECT arrayMap(plus(_, 5) | plus(_, 5) | plus(_, 5), [1, 2, 3]);
SELECT arrayMap(plus(_, _) | negate(_), [1, 2, 3], [4, 5, 6]);

-- Bare function names as operands. A variadic function can be used on the right
-- (a composition applies it to exactly one value), but not on the left.
SELECT arrayMap(negate | toString, [1, 2, 3]);
SELECT arrayMap(negate | abs | toString, [1, 2, 3]);
SELECT arrayMap(plus(_, 1) | toString, [1, 2]);

-- A bare placeholder operand is the identity function.
SELECT arrayMap(_1 | plus(_, 1), [1, 2]);
SELECT arrayMap(plus(_, 1) | _1, [1, 2]);
SELECT arrayMap(_ | toString, [1, 2]);

-- Lambdas bound to names.
WITH (x -> x + 1) AS inc SELECT arrayMap(inc | inc, [1, 2, 3]);

-- Columns are captured as usual.
SELECT arrayMap(plus(_, x) | multiply(_, x), [1, 2]) FROM (SELECT 10 AS x);

-- A column with the same name as an argument of the left operand is not captured by it.
SELECT arrayMap((x -> x * 2) | (y -> y + x), [1, 2, 3]) FROM (SELECT 100 AS x);

-- A subquery inside an operand that only reuses the name of a lambda argument locally is
-- unaffected by the substitution.
SELECT arrayMap((x -> x + 1) | (x -> x + (SELECT max(x) FROM (SELECT 1 AS x))), [1, 2]);
SELECT arrayMap((x -> x + 1) | (x -> x + (SELECT sum(x) FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x))), [1]);

-- Member access on the result of the left operand.
SELECT arrayMap((x -> (x, x + 1)) | (t -> t.2), [1, 2, 3]);
SELECT arrayMap((x -> CAST((x, x + 1), 'Tuple(a Int64, b Int64)')) | (t -> t.b), [1, 2, 3]);

-- Compositions work in every higher-order function and fold to constants as usual.
SELECT arrayFilter(modulo(_, 2) | equals(_, 1), [1, 2, 3, 4, 5]);
SELECT arraySum(arrayMap(plus(_, 1) | multiply(_, 2), [1, 2, 3]));

-- The operator formats as the internal `__compose` function.
SELECT formatQuery('SELECT arrayMap(plus(_, 1) | multiply(_, 2), [1, 2, 3])');

-- The right operand must be a function of one argument.
SELECT arrayMap(negate(_) | plus(_, _), [1, 2, 3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Operands must be functions.
SELECT arrayMap(5 | plus(_, 5), [1, 2, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(plus(_, 5) | 5, [1, 2, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(now() | toString(_), [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(no_such_function_05038 | toString(_), [1]); -- { serverError BAD_ARGUMENTS }

-- Substituting into a subquery that references the name is not supported.
SELECT arrayMap((x -> x + 1) | (x -> x + (SELECT sum(number * x) FROM numbers(3))), [1]); -- { serverError NOT_IMPLEMENTED }

-- The arity of a variadic left operand cannot be inferred: use placeholders.
SELECT arrayMap(concat | length, ['a', 'b']); -- { serverError BAD_ARGUMENTS }

-- The single `|` is function composition, not bitwise OR.
SELECT 1 | 2; -- { serverError BAD_ARGUMENTS }

-- A composition can be used only where a function is expected.
SELECT plus(_, 1) | negate(_); -- { serverError BAD_ARGUMENTS }
SELECT length(plus(_, 1) | negate(_)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
