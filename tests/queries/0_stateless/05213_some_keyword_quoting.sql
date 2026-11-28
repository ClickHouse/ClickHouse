-- `SOME` is a valid identifier, but on the right-hand side of a comparison the parser consumes it
-- as the array-quantifier keyword. Left unquoted by the formatter, a function named `SOME` is read
-- back as that quantifier and the expression is rewritten to `arrayExists`, so the formatted AST
-- does not match the original one.

SELECT formatQuerySingleLine('SELECT 1 < SOME(a, b -> 1)');

-- Formatting has to be idempotent: the first pass parenthesises the lambda parameters, which is
-- what let the second pass read `SOME` as the quantifier.
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT 1 < SOME(a, b -> 1)'))
     = formatQuerySingleLine('SELECT 1 < SOME(a, b -> 1)');

-- The parser also drops the `ALL` aggregate qualifier, which turned the argument list into a
-- single expression on the way back, so this reaches the same rewrite without a lambda.
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT 1 < SOME(ALL 2)'))
     = formatQuerySingleLine('SELECT 1 < SOME(ALL 2)');

-- The quantifier also has a subquery form, so the same break is reachable without an array.
SELECT formatQuerySingleLine('SELECT 1 = `SOME`((SELECT 1))');

-- `ALL` is the quantifier paired with `SOME` and was already quoted; the two now behave alike.
SELECT formatQuerySingleLine('SELECT 1 < ALL(a, b -> 1)');

-- Quoting is by exact name, in any case, and a name that merely starts like it is left alone.
SELECT formatQuerySingleLine('SELECT `some`, `SOME`, `Some`, somex, some_1');

-- The array quantifier is rewritten at parse time, so the keyword never reaches the formatter as
-- a name and the feature is unaffected.
SELECT formatQuerySingleLine('SELECT 3 = SOME([1, 2, 3])');
SELECT formatQuerySingleLine('SELECT 5 < SOME([1, 2, 6])');
