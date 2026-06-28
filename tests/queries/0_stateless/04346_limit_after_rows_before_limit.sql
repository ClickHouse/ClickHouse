-- rows_before_limit_at_least must reflect the rows read before the AFTER/UNTIL range, i.e. the full
-- input the LimitRangeTransform scanned, even when an outer settings LimitStep sits downstream.
-- A downstream settings LimitStep must not shadow the range transform's counter (which would report
-- rows after the range instead of before it). This must match normal LIMIT semantics: 10, not 3.

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
