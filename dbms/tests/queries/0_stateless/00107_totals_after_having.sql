SET max_rows_to_group_by = 100000;
SET max_block_size = 100001;
SET group_by_overflow_mode = 'any';

SET totals_mode = 'after_having_auto';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_inclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_exclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'before_having';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT number FROM system.numbers LIMIT 500000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;
