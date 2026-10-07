-- STID 1941-1bfa: a parenthesized + aliased lambda used as the operand of an
-- access operator (tupleElement `.N`, arrayElement `[]`) and placed at a
-- non-first position of an expression list aborted the server with
-- `Inconsistent AST formatting` LOGICAL_ERROR in debug / sanitizer builds.
--
-- Root cause: `FormatStateStacked::list_element_index` records a node's index
-- among the direct elements of the enclosing expression list and is only
-- meaningful one level deep (the lambda formatter self-parenthesizes when the
-- index is > 0 so that `f(x, y -> z)` is not misformatted as `f((x, y) -> z)`).
-- `ASTFunction::formatImplWithoutAlias` derived its `nested_*_parens` frames
-- from the parent frame without clearing `list_element_index`, so the index of
-- the outermost SELECT-list element leaked down through the access operators
-- into the nested lambda. The lambda then emitted an extra pair of parens, and
-- because the access operator also wrapped it, the format-parse-format
-- round-trip diverged by one paren level (`((expr) AS a)` vs `(expr AS a)`).
--
-- The fix resets `list_element_index` to 0 on the nested frames; the argument
-- list loops that legitimately need the index re-set it explicitly per
-- argument, so the lambda-as-direct-list-element case is unaffected.

-- Original reproducer. The query is semantically invalid (a lambda cannot be a
-- tupleElement operand), so it fails with ILLEGAL_TYPE_OF_ARGUMENT. We only
-- care that the server does not abort with LOGICAL_ERROR before reaching that
-- point, i.e. the internal AST round-trip check passes.
SELECT 1, ((p0, p1) -> p0 AS a7).4[3] FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT z, ((p0, p1) -> p0 AS a7).4[3][3] FROM numbers(1); -- { serverError UNKNOWN_IDENTIFIER, ILLEGAL_TYPE_OF_ARGUMENT }

-- The formatted output must be stable across a format-parse-format round-trip.
-- Before the fix the first format produced `((expr) AS a7)` (double parens) and
-- the re-parse+reformat produced `(expr AS a7)`, differing by one paren level.
SELECT formatQuerySingleLine('SELECT 1, ((p0, p1) -> p0 AS a7).4[3] FROM t');
SELECT formatQuerySingleLine('SELECT z, ((p0, p1) -> p0 AS a7).4[3][3] FROM t');
SELECT formatQuerySingleLine('SELECT a, b, (x -> x AS l).1 FROM t');
SELECT formatQuerySingleLine('SELECT 1, 2, ((p) -> p AS f)[1] FROM t');
SELECT formatQuerySingleLine('SELECT m[1], (x -> x AS g).3[2] FROM t');

-- A lambda that is the first element of the SELECT list must keep working —
-- the access-operator path used to be correct only in this position.
SELECT formatQuerySingleLine('SELECT (x -> x AS l).1, a FROM t');

-- Lambda as a direct (non-first) SELECT-list element must still self-
-- parenthesize: `SELECT 1, x -> x` -> `SELECT 1, (x -> x)`. The fix only
-- clears the leaked index for operands reached through an operator, not for
-- direct list elements (ASTExpressionList sets the index right before
-- formatting the child).
SELECT formatQuerySingleLine('SELECT 1, x -> x FROM t');

-- Lambda as a non-first function argument must still be parenthesized:
-- `f(x, y -> z)` must not become `f((x, y) -> z)` semantics. The argument-list
-- loop re-sets list_element_index per argument after the reset.
SELECT formatQuerySingleLine('SELECT f(a, (x, y) -> x) FROM t');
SELECT formatQuerySingleLine('SELECT arrayFilter((x, y) -> x, [1], [2])');

-- A valid, executable shape with an aliased lambda passed to a higher-order
-- function at a non-first list position, plus the result indexed. Must execute
-- cleanly (round-trip check passes) and return the expected value.
SELECT 1, arrayMap(x -> x, [10, 20, 30])[2] FROM numbers(1);
SELECT 1, arrayMap((p0, p1) -> p0, [5], [6]) AS a7 FROM numbers(1);

-- Aliased lambda under unary / binary operators at a non-first position
-- (these also format operands through the nested frames).
SELECT formatQuerySingleLine('SELECT 1, -(x -> x AS q).1 FROM t');
SELECT formatQuerySingleLine('SELECT 1, NOT (x -> x AS q).1 FROM t');
SELECT formatQuerySingleLine('SELECT 1, (x -> x AS q).1 + 2 FROM t');
SELECT formatQuerySingleLine('SELECT 1, arrayMap(x -> arrayMap(y -> y AS inner, [x]), [1, 2]) FROM t');
