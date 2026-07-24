DROP TABLE IF EXISTS numbers_indexed;
DROP TABLE IF EXISTS squares;

CREATE TABLE numbers_indexed Engine=MergeTree ORDER BY number PARTITION BY bitShiftRight(number,8) SETTINGS index_granularity=8 AS SELECT * FROM numbers(16384);

CREATE VIEW squares AS WITH number*2 AS square_number SELECT number, square_number FROM numbers_indexed;

SET max_rows_to_read=8, read_overflow_mode='throw';

WITH number * 2 AS square_number SELECT number, square_number FROM numbers_indexed WHERE number = 999;

SELECT * FROM squares WHERE number = 999;

EXPLAIN SYNTAX SELECT number, square_number FROM ( WITH number * 2 AS square_number SELECT number, square_number FROM numbers_indexed) AS squares WHERE number = 999;

DROP TABLE IF EXISTS squares;
DROP TABLE IF EXISTS numbers_indexed;
