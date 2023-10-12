DROP TABLE IF EXISTS t_leading_zeroes;
CREATE TABLE t_leading_zeroes(ref Float64, val INTEGER) ENGINE=MergeTree ORDER BY ref;

INSERT INTO t_leading_zeroes VALUES (-0.02, 00000), (0, 0), (0.01, 00), (0.02, 01), (0.03, +01), (0.04, -01), (1, 0001), (2, 0005), (3, 0008), (4, 0017), (5, 0021), (6, 0051), (7, 00000123), (8, 0b10000), (9, 0x0abcd);

SELECT * FROM t_leading_zeroes;

DROP TABLE IF EXISTS t_leading_zeroes;
