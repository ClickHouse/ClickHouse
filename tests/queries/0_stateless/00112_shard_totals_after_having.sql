SET totals_mode = 'after_having_auto';
SET max_rows_to_group_by = 100000;
SET group_by_overflow_mode = 'any';
SELECT dummy + 1 AS k, count() FROM remote('127.0.0.{2,3}', system, one) GROUP BY k WITH TOTALS ORDER BY k;
