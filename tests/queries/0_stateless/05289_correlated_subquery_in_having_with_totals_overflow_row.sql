-- A correlated subquery in `HAVING` together with the aggregation overflow row (`WITH TOTALS`,
-- `max_rows_to_group_by`, `group_by_overflow_mode = 'any'` and `totals_mode` other than
-- `after_having_exclusive`) used to throw the logical error `Chunk should have AggregatedChunkInfo in
-- TotalsHavingTransform`: the decorrelation joins the aggregated stream and drops the chunk infos.
-- The combination is rejected now.

SET enable_analyzer = 1;
SET max_rows_to_group_by = 5, group_by_overflow_mode = 'any';

SELECT number, count() FROM numbers(10) GROUP BY number WITH TOTALS HAVING ((SELECT number) % 3) = 0 ORDER BY number
SETTINGS totals_mode = 'before_having'; -- { serverError NOT_IMPLEMENTED }
SELECT number, count() FROM numbers(10) GROUP BY number WITH TOTALS HAVING ((SELECT number) % 3) = 0 ORDER BY number
SETTINGS totals_mode = 'after_having_inclusive'; -- { serverError NOT_IMPLEMENTED }
SELECT number, count() FROM numbers(10) GROUP BY number WITH TOTALS HAVING ((SELECT number) % 3) = 0 ORDER BY number
SETTINGS totals_mode = 'after_having_auto'; -- { serverError NOT_IMPLEMENTED }

SELECT 'without the overflow row';
SELECT number, count() FROM numbers(10) GROUP BY number WITH TOTALS HAVING ((SELECT number) % 3) = 0 ORDER BY number
SETTINGS totals_mode = 'after_having_exclusive';

SELECT 'without the correlated subquery';
SELECT number, count() FROM numbers(10) GROUP BY number WITH TOTALS HAVING (number % 3) = 0 ORDER BY number
SETTINGS totals_mode = 'after_having_auto';
