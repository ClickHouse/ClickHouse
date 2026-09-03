SET max_rows_to_group_by = 100000;
SET group_by_overflow_mode = 'any';

-- 'any' overflow mode might select different values for two-level and
-- single-level GROUP BY, so we set a big enough threshold here to ensure that
-- the switch doesn't happen, we only use single-level GROUP BY and get a
-- predictable result.
SET group_by_two_level_threshold_bytes = 100000000;
SET group_by_two_level_threshold = 1000000;

SET totals_mode = 'after_having_auto';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'after_having_inclusive';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'after_having_exclusive';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'before_having';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;
