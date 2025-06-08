CREATE TABLE t (st FixedString(54)) ENGINE=MergeTree ORDER BY ();

INSERT INTO t VALUES 
('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ'),
('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'),
('IIIIIIIIII\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0');

WITH (SELECT groupConcat(',')(st) FROM t) AS a,
     (SELECT groupConcat(',')(st :: String) FROM t) AS b
SELECT equals(a, b);
