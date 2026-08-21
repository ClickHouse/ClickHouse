SET enable_analyzer = 1;

-- These settings force the left-column removal path (canRemoveColumnsFromLeftBlock): a nonzero
-- external-join threshold treats the join as a possible grace-hash and disables removal, and disabling
-- query_plan_remove_unused_columns keeps both duplicate left columns so the header count matches anyway.
-- Any of them would hide the bug under randomized settings.
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_remove_unused_columns = 1;

SELECT a, b FROM (
    SELECT 42 AS a, 1 AS b, 1 AS b
    UNION ALL
    SELECT 42 AS a, 1 AS b, 1 AS b
) AS l RIGHT JOIN (SELECT 99 AS a) AS r USING (a) ORDER BY ALL;

SELECT '---';

SELECT a, b FROM (
    SELECT 42 AS a, 1 AS b, 1 AS b
    UNION ALL
    SELECT 42 AS a, 1 AS b, 1 AS b
) AS l FULL JOIN (SELECT 99 AS a) AS r USING (a) ORDER BY ALL;

SELECT '---';

-- Matched and unmatched right rows together, distinct left values, checked across join algorithms.
SELECT a, b FROM (
    SELECT 42 AS a, 1 AS b, 1 AS b
    UNION ALL
    SELECT 42 AS a, 5 AS b, 5 AS b
) AS l RIGHT JOIN (SELECT 42 AS a UNION ALL SELECT 99 AS a) AS r USING (a) ORDER BY ALL
SETTINGS join_algorithm = 'hash';

SELECT '---';

SELECT a, b FROM (
    SELECT 42 AS a, 1 AS b, 1 AS b
    UNION ALL
    SELECT 42 AS a, 5 AS b, 5 AS b
) AS l RIGHT JOIN (SELECT 42 AS a UNION ALL SELECT 99 AS a) AS r USING (a) ORDER BY ALL
SETTINGS join_algorithm = 'parallel_hash';

SELECT '---';

-- Duplicate output columns must still be preserved when they are genuinely requested.
SELECT a, b, b FROM (SELECT 42 AS a, 1 AS b, 1 AS b) AS l LEFT JOIN (SELECT 42 AS a) AS r USING (a) ORDER BY ALL;
