-- A non-literal constant aggregate-function parameter (e.g. `0.4 + 0.1`) is evaluated through the
-- `Field`-returning `evaluateConstantExpression` compatibility bridge (`materializeToField`), which
-- reads the value via the column's `operator[]`. This asserts the parameter value is preserved, so
-- the aggregate behaves exactly as if a folded literal had been passed. See PR #114666.

-- Float parameter: `quantile` level 0.4 + 0.1 = 0.5 -> median of 0..10.
SELECT quantile(0.4 + 0.1)(number) FROM numbers(11);
SELECT quantileExact(0.2 * 5)(number) FROM numbers(11);

-- Integer parameter: `groupArray` max size 1 + 1 = 2.
SELECT groupArray(1 + 1)(number) FROM numbers(10);

-- The same holds on the old analyzer.
SELECT quantile(0.4 + 0.1)(number) FROM numbers(11) SETTINGS enable_analyzer = 0;
SELECT groupArray(1 + 1)(number) FROM numbers(10) SETTINGS enable_analyzer = 0;
