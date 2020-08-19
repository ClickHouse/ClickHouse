SET max_rows_to_group_by = 100000;
SET group_by_overflow_mode = 'any';

SET totals_mode = 'after_having_auto';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'after_having_inclusive';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'after_having_exclusive';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;

SET totals_mode = 'before_having';
SELECT dummy, count() GROUP BY dummy WITH TOTALS;
