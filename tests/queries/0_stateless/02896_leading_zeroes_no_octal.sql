DROP TABLE IF EXISTS t_leading_zeroes;
DROP TABLE IF EXISTS t_leading_zeroes_f;

CREATE TABLE t_leading_zeroes(id INTEGER, input String, val INTEGER, expected INTEGER) ENGINE=MergeTree ORDER BY id;

INSERT INTO t_leading_zeroes VALUES (1, '00000', 00000, 0), (2, '0', 0, 0), (3, '00', 00, 0), (4, '01', 01, 1), (5, '+01', +01, 1);
INSERT INTO t_leading_zeroes VALUES (6, '-01', -01, -1), (7, '0001', 0001, 1), (8, '0005', 0005, 5), (9, '0008', 0008, 8);
INSERT INTO t_leading_zeroes VALUES (10, '0017', 0017, 17), (11, '0021', 0021, 21), (12, '0051', 0051, 51), (13, '00000123', 00000123, 123);
INSERT INTO t_leading_zeroes VALUES (14, '0b10000', 0b10000, 16), (15, '0x0abcd', 0x0abcd, 43981), (16, '0000.008', 0000.008, 0)
INSERT INTO t_leading_zeroes VALUES (17, '1000.0008', 1000.0008, 1000), (18, '0008.0008', 0008.0008, 8);

SELECT 'Leading zeroes into INTEGER';
SELECT t.val == t.expected AS ok, * FROM t_leading_zeroes t ORDER BY id;

-- Floats don't go via the weird octal path:
CREATE TABLE t_leading_zeroes_f(id INTEGER, input String, val Float32, expected Float32) ENGINE=MergeTree ORDER BY id;
INSERT INTO t_leading_zeroes_f VALUES (1, '00000', 00000, 0), (2, '00009.00009', 00009.00009, 9.00009), (3, '00009e9', 00009e9, 9e9), (4, '00009e09', 00009e09, 9e9), (5, '00009e0009', 00009e0009, 9e9);
INSERT INTO t_leading_zeroes_f VALUES (6, '-00000', -00000.1, -0.1), (7, '-00009.00009', -00009.00009, -9.00009), (8, '-00009e9', -00009e9, -9e9), (9, '-00009e09', -00009e09, -9e9), (10, '-00009e0009', -00009e0009, -9e9);
INSERT INTO t_leading_zeroes_f VALUES (11, '+00000', +00000., 0), (12, '+00009.00009', +00009.00009, 9.00009), (13, '+00009e9', +00009e9, 9e9), (14, '+00009e09', +00009e09, 9e9), (15, '+00009e0009', +00009e0009, 9e9);
-- Coincidentally, the following result in 9 rather than 9e9 because of readFloatTextFastImpl
-- using readUIntTextUpToNSignificantDigits<4>(exponent, in)
-- INSERT INTO t_leading_zeroes_f VALUES (100, '00009e00009', 00009e00009, 9e9), (101, '-00009e00009', -00009e00009, -9e9), (102, '+00009e00009', +00009e00009, 9e9)

SELECT 'Leading zeroes into Float32';
SELECT t.val == t.expected AS ok, * FROM t_leading_zeroes_f t ORDER BY id;

DROP TABLE IF EXISTS t_leading_zeroes;
DROP TABLE IF EXISTS t_leading_zeroes_f;