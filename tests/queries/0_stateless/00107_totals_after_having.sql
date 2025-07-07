SELECT '*** In-memory aggregation.';

SET max_rows_to_group_by = 100000;
SET max_block_size = 100001;
SET group_by_overflow_mode = 'any';

-- 'any' overflow mode might select different values for two-level and
-- single-level GROUP BY, so we set a big enough threshold here to ensure that
-- the switch doesn't happen, we only use single-level GROUP BY and get a
-- predictable result.
SET group_by_two_level_threshold_bytes = 100000000;
SET group_by_two_level_threshold = 1000000;

SELECT '**** totals_mode = after_having_auto';
SET totals_mode = 'after_having_auto';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = after_having_inclusive';
SET totals_mode = 'after_having_inclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = after_having_exclusive';
SET totals_mode = 'after_having_exclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = before_having';
SET totals_mode = 'before_having';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;


SELECT '*** External aggregation.';

SET max_bytes_ratio_before_external_group_by = 0;
SET max_bytes_before_external_group_by = 1000000;
SET group_by_two_level_threshold = 100000;

SELECT '**** totals_mode = after_having_auto';
SET totals_mode = 'after_having_auto';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = after_having_inclusive';
SET totals_mode = 'after_having_inclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = after_having_exclusive';
SET totals_mode = 'after_having_exclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '**** totals_mode = before_having';
SET totals_mode = 'before_having';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;
