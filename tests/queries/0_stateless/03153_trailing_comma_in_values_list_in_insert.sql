CREATE TEMPORARY TABLE test (a UInt8, b UInt8, c UInt8);
INSERT INTO test (a, b, c) VALUES (1, 2, 3, );
INSERT INTO test (a, b, c) VALUES (4, 5, 6,);
INSERT INTO test (a, b, c) VALUES (7, 8, 9);
SELECT * FROM test ORDER BY a;
