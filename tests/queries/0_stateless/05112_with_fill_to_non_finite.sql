-- A `WITH FILL TO` bound of `NaN` or `Inf` is one the fill loop can never pass, so the query generated
-- fill rows until it hit a time limit, and forever without one. Such a bound is rejected now, like the
-- other unusable `WITH FILL` parameters.

SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO nan STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO inf STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL TO -inf STEP -1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO nan STEP 1) SETTINGS enable_analyzer = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a finite bound still fills';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO 10 STEP 1);
SELECT x FROM (SELECT toFloat64(number) AS x FROM numbers(2)) ORDER BY x WITH FILL FROM 0 TO 4 STEP 1;

SELECT 'a TO bound with no STEP is checked as well';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO nan); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'FROM and STEP keep their existing behaviour, which the fill loop guards at run time';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL FROM nan STEP 1);
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO 5 STEP nan);

-- `STALENESS` feeds the same fill loop constraint as `TO`, so a non-finite value there does not terminate either.
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS nan); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL STEP -1 STALENESS -inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS nan) SETTINGS enable_analyzer = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a finite staleness still fills';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS 2);
