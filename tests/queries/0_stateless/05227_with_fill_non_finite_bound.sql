-- No `WITH FILL` parameter is usable as `NaN` or `Inf`. A `TO` bound towards the fill direction leaves the
-- loop's termination test always true and the query generated fill rows until it hit a time limit, and
-- forever without one. The rest do not hang but fill nothing, and a non-finite `FROM` also loses the fill
-- rows that a finite `TO` asks for. All of them are rejected now.

SELECT 'a TO bound towards the fill direction generated rows without end';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO nan STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO inf STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL TO -inf STEP -1); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a TO bound against the fill direction filled nothing';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO -inf STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL TO nan STEP -1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL TO inf STEP -1); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a TO bound with no STEP is checked as well';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO nan); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- `STALENESS` feeds the same loop constraint as `TO`, through `updateConstraintsWithStalenessRow`.
SELECT 'a STALENESS value reaches the same constraint as TO';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS nan); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL STEP -1 STALENESS -inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- A non-finite `FROM` did not hang: it emitted the bound itself as a row and then stopped on the guards
-- inside `FillingRow::next`, so `FROM -inf TO 3 STEP 1` over the rows 0 and 1 returned `-inf, 0, 1` and
-- dropped the 2 that `FROM -1 TO 3 STEP 1` fills in.
SELECT 'a FROM bound left itself in the result and lost the rest of the fill';
SELECT x FROM (SELECT toFloat64(number) AS x FROM numbers(2)) ORDER BY x WITH FILL FROM -inf TO 3 STEP 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL FROM nan STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL FROM inf STEP -1); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- A non-finite `STEP` makes the cursor non-finite on the very first step, which is where the guards
-- inside `FillingRow::next` stop the fill, so `FROM 0 TO 4 STEP nan` over the rows 0 and 1 returned just
-- those two rows instead of the four that `STEP 1` fills in.
SELECT 'a STEP value that is not finite filled nothing';
SELECT x FROM (SELECT toFloat64(number) AS x FROM numbers(2)) ORDER BY x WITH FILL FROM 0 TO 4 STEP nan; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO 5 STEP inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x DESC WITH FILL TO -5 STEP -inf); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a bound is checked after it is evaluated, not only as a literal';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO -log(0) STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO CAST('inf', 'Float32') STEP 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'finite values still fill';
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL TO 10 STEP 1);
SELECT x FROM (SELECT toFloat64(number) AS x FROM numbers(2)) ORDER BY x WITH FILL FROM 0 TO 4 STEP 1;
SELECT x FROM (SELECT toFloat64(number) AS x FROM numbers(2)) ORDER BY x WITH FILL FROM -1 TO 3 STEP 1;
SELECT count() FROM (SELECT toFloat64(number) AS x FROM numbers(3) ORDER BY x WITH FILL STEP 1 STALENESS 2);
