-- Tags: shard, no-fasttest

SET max_rows_to_group_by = 100000;
SET max_block_size = 100001;
SET group_by_overflow_mode = 'any';

-- Settings 'max_rows_to_group_by', 'max_bytes_before_external_group_by' and 'max_bytes_ratio_before_external_group_by' are mutually exclusive.
SET max_bytes_ratio_before_external_group_by = 0;
SET max_bytes_before_external_group_by = 0;

DROP TABLE IF EXISTS numbers500k;
CREATE TABLE  numbers500k (number UInt32) ENGINE = TinyLog;

INSERT INTO numbers500k SELECT number FROM system.numbers LIMIT 500000;

SET totals_mode = 'after_having_auto';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT * FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) ORDER BY number) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_inclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT * FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) ORDER BY number) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_exclusive';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT * FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) ORDER BY number) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'before_having';
SELECT intDiv(number, 2) AS k, count(), argMax(toString(number), number) FROM (SELECT * FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) ORDER BY number) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

DROP TABLE numbers500k;
