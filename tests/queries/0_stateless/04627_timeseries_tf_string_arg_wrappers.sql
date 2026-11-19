-- Tags: no-fasttest
-- ^^ PromQL grammar needs ANTLR4 (disabled in fast-test).
--
-- The `prometheusQuery` / `timeSeriesSelector` table functions read their String arguments via
-- `evaluateConstantExpressionAsColumn` (no `Field`). This pins the compatibility contract the
-- previous `Field`-based code had: a non-NULL `Nullable(String)` / `LowCardinality(String)` constant
-- is accepted (the wrapper is unwrapped), while a NULL value is rejected with BAD_ARGUMENTS.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- prometheusQuery: Nullable(String) / LowCardinality(String) accepted for table, promql and database args
SELECT value FROM prometheusQuery(CAST('ts', 'Nullable(String)'), '1 + 2', 1000);
SELECT value FROM prometheusQuery(toLowCardinality('ts'), '1 + 2', 1000);
SELECT value FROM prometheusQuery('ts', CAST('1 + 2', 'Nullable(String)'), 1000);
SELECT value FROM prometheusQuery(CAST(currentDatabase(), 'Nullable(String)'), 'ts', '1 + 2', 1000);
-- prometheusQuery: NULL string argument rejected
SELECT value FROM prometheusQuery(CAST(NULL, 'Nullable(String)'), '1 + 2', 1000); -- { serverError BAD_ARGUMENTS }
SELECT value FROM prometheusQuery('ts', CAST(NULL, 'Nullable(String)'), 1000); -- { serverError BAD_ARGUMENTS }

-- timeSeriesSelector: same contract (empty table -> 0 rows)
SELECT count() FROM timeSeriesSelector(CAST('ts', 'Nullable(String)'), 'up', 0, 1000);
SELECT count() FROM timeSeriesSelector(toLowCardinality('ts'), 'up', 0, 1000);
SELECT count() FROM timeSeriesSelector('ts', CAST('up', 'Nullable(String)'), 0, 1000);
-- timeSeriesSelector: NULL string argument rejected
SELECT count() FROM timeSeriesSelector(CAST(NULL, 'Nullable(String)'), 'up', 0, 1000); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM timeSeriesSelector('ts', CAST(NULL, 'Nullable(String)'), 0, 1000); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts;
