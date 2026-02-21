-- Validate that IN checks column count mismatch between left tuple and right subquery during analysis.
-- https://github.com/ClickHouse/ClickHouse/issues/74442

SELECT (1, 1) IN (SELECT 1); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT 1 IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT (1, 1, 1) IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

SELECT (1, 1) IN (SELECT 1, 2);

-- Original reproducer from the issue: previously silently returned empty result, now should error.
SELECT 1 FROM (SELECT 2 AS c1 WHERE (1, 1) IN (SELECT 1)) t0 WHERE t0.c1 = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
