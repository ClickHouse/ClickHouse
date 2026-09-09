-- A constant condition prefix in `multiIf`, and identical constant branches in `if`, make the whole
-- expression constant even though not every condition is constant. `LowCardinality` gives no benefit
-- for a constant, and functions that opt out of the default `LowCardinality` implementation expect
-- their constant string arguments as `Const(String)`, so these cases must keep plain `String`.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'A constant true condition prefix in multiIf keeps String';
SELECT multiIf(1, 'max', number = 1, 'sum', 'avg') AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(0, 'a', 1, 'max', number = 1, 'sum', 'avg') AS res, toTypeName(res) FROM numbers(2);

SELECT 'Identical constant branches in if keep String';
WITH 'sum' AS f SELECT if(number % 2, f, f) AS res, toTypeName(res) FROM numbers(2);
WITH CAST(NULL AS Nullable(String)) AS f SELECT if(number % 2, f, f) AS res, toTypeName(res) FROM numbers(2);

-- Note: these expressions are not tested with `arrayReduce` and friends. Unlike the fully constant
-- cases of `04628_if_const_strings_lowcardinality_fully_const`, their constness is only established
-- during execution, so `arrayReduce(multiIf(1, 'sum', number = 1, 'max', 'min'), [1, 2, 3])` is
-- rejected as a non-constant aggregate function name regardless of this optimization.

SELECT 'A non-constant condition prefix still gets LowCardinality';
SELECT multiIf(0, 'a', number = 1, 'b', 'c') AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(number = 1, 'b', 1, 'a', 'c') AS res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2, 'x', 'y') AS res, toTypeName(res) FROM numbers(2);
