-- Regression test: a lambda with an alias inside a multi-element WITH list must
-- format and re-format identically. Previously the lambda formatter wrapped the
-- lambda in `(...)` whenever `frame.list_element_index > 0`; on the second pass
-- `IAST::format` also wrote its own `(` because of the parser-set
-- `parenthesized` flag, producing a different output.

SELECT formatQuerySingleLine('WITH (functor, x) -> functor(x) AS lambda, x -> x + 1 AS functor_1, x -> toString(x) AS functor_2 SELECT lambda(functor_1, 1), lambda(functor_2, 1)');
SELECT formatQuerySingleLine('WITH x -> x + 1 AS functor_1, x -> x + 2 AS functor_2 SELECT functor_1(1), functor_2(1)');
SELECT formatQuerySingleLine('SELECT arrayMap(x -> x + 1, [1, 2, 3]), arrayMap(y -> y - 1, [4, 5, 6])');
