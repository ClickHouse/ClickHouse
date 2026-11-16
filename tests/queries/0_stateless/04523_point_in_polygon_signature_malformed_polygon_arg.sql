-- The declarative signature of `pointInPolygon` describes the polygon arguments with a bare
-- `Array` matcher (their full contract — nesting depth, innermost tuple arity, element types and
-- cross-argument consistency — is not expressible in the DSL). The authoritative validation lives
-- in `getReturnTypeImpl`, which must run on the column path; otherwise a malformed polygon argument
-- slips through analysis and aborts inside `executeImpl` while reading a missing coordinate.
-- Regression for a crash found by the AST fuzzer on https://github.com/ClickHouse/ClickHouse/pull/104948.

SET validate_polygons = 0;

-- An array of one-element tuples as the first polygon argument (the exact fuzzer case): it used to
-- reach `executeImpl` and abort while reading the absent y coordinate. It must be rejected cleanly.
SELECT pointInPolygon((9223372036854775806, -2147483647), [tuple(0.9999)], [(-2147483648, 100), (2, 65537), (0, 2)]); -- { serverError BAD_ARGUMENTS }

-- The same malformed shape in the single-polygon form.
SELECT pointInPolygon((1, 1), [tuple(0.9999)]); -- { serverError BAD_ARGUMENTS }

-- A three-element tuple is equally invalid.
SELECT pointInPolygon((1, 1), [(1, 2, 3)]); -- { serverError BAD_ARGUMENTS }

-- Non-numeric tuple elements.
SELECT pointInPolygon((1, 1), [('a', 'b')]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An array whose elements are not tuples.
SELECT pointInPolygon((1, 1), [1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Nesting deeper than a multi-polygon (depth 4).
SELECT pointInPolygon((1, 1), [[[[(1, 1), (2, 2), (3, 3)]]]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Well-formed calls still work.
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]);
SELECT pointInPolygon((2., 2.), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]);
