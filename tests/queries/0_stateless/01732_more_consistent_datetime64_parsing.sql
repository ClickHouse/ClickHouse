-- An unquoted number is a Unix timestamp in seconds (the fractional part, if any, gives sub-second
-- precision), so the integer and decimal forms below denote the same instant up to the fraction.
CREATE TEMPORARY TABLE t (i UInt8, x DateTime64(3, 'UTC'));
INSERT INTO t VALUES (1, 1111111111);
INSERT INTO t VALUES (2, 1111111111.222);
INSERT INTO t VALUES (3, '1111111111222');
INSERT INTO t VALUES (4, '1111111111.222');
SELECT * FROM t ORDER BY i;

SELECT toDateTime64(1111111111.222, 3, 'Asia/Istanbul');
SELECT toDateTime64('1111111111.222', 3, 'Asia/Istanbul');
SELECT toDateTime64('1111111111222', 3, 'Asia/Istanbul');
SELECT ignore(toDateTime64(1111111111222, 3, 'Asia/Istanbul')); -- This gives somewhat correct but unexpected result
