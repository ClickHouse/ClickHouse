-- Without the `limit` setting, rows_before_limit_at_least is owned by the range transform and counts
-- every row it read before the AFTER/UNTIL range was applied, as for a plain LIMIT: all 10 rows here.
-- The `limit` setting is applied as an outer LIMIT wrapped around the whole query (see
-- `applyQueryConstructionSettings`), so with it the counter belongs to that outer LIMIT and reports the
-- rows the range produced, exactly as an explicit outer `SELECT * FROM (...) LIMIT n` would.

SET output_format_write_statistics = 0;
SET exact_rows_before_limit = 1;

-- { echo }

-- LIMIT AFTER alone: counter on the range transform, counts all 10 input rows.
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5 FORMAT JSONCompact;

-- LIMIT AFTER with a settings limit downstream: the settings LimitStep must not shadow the counter.
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5 FORMAT JSONCompact SETTINGS limit = 1;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5 FORMAT JSONCompact SETTINGS limit = 1, enable_analyzer = 0;

-- LIMIT UNTIL with a settings limit downstream.
SELECT number FROM numbers(10) ORDER BY number LIMIT UNTIL number >= 7 FORMAT JSONCompact SETTINGS limit = 1;
SELECT number FROM numbers(10) ORDER BY number LIMIT UNTIL number >= 7 FORMAT JSONCompact SETTINGS limit = 1, enable_analyzer = 0;
