-- Identical constant branches make the result of `multiIf` that very constant, regardless of the
-- conditions - exactly as `if` already recognizes for `if(cond, x, x)`. `multiIf` must therefore keep
-- plain `String` too: otherwise the two result types diverge and `optimize_multiif_to_if`, which
-- rewrites only when they match, silently stops rewriting `multiIf(cond, x, x)`.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;
SET optimize_multiif_to_if = 1;

SELECT 'Identical constant branches keep String in both if and multiIf';
WITH 'sum' AS f SELECT multiIf(number % 2, f, f) AS res, toTypeName(res) FROM numbers(2);
WITH 'sum' AS f SELECT if(number % 2, f, f) AS res, toTypeName(res) FROM numbers(2);

SELECT 'Identical constant branches keep String for more than one condition too';
WITH 'sum' AS f SELECT multiIf(number % 2, f, number % 3, f, f) AS res, toTypeName(res) FROM numbers(2);

SELECT 'Identical constant NULL branches keep their type';
WITH CAST(NULL AS Nullable(String)) AS f SELECT multiIf(number % 2, f, f) AS res, toTypeName(res) FROM numbers(2);

SELECT 'Distinct constant branches still get LowCardinality';
SELECT multiIf(number % 2, 'sum', 'max') AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(number % 2, 'sum', number % 3, 'max', 'sum') AS res, toTypeName(res) FROM numbers(2);

-- Note: as in `04650_if_const_strings_lowcardinality_const_prefix`, these expressions cannot be tested
-- with `arrayReduce` and friends - their constness is only established during execution, so
-- `arrayReduce(multiIf(number % 2, f, f), [1, 2, 3])` is rejected as a non-constant aggregate function
-- name regardless of this optimization.
