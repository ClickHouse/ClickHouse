SET max_rows_to_group_by = 100000;
SET max_block_size = 100001;
SET group_by_overflow_mode = 'any';

DROP TABLE IF EXISTS test.numbers500k;
CREATE VIEW test.numbers500k AS SELECT number FROM system.numbers LIMIT 500000;

SET totals_mode = 'after_having_auto';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM remote('127.0.0.{2,3}', test, numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_inclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM remote('127.0.0.{2,3}', test, numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_exclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM remote('127.0.0.{2,3}', test, numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'before_having';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM remote('127.0.0.{2,3}', test, numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

DROP TABLE test.numbers500k;
