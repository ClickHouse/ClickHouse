SELECT TreeHeight (from, to) FROM (SELECT * FROM VALUES('from UInt64, to UInt64', (1, 1)) WHERE 0);
SELECT TreeHeight (from, to) FROM VALUES('from UInt64, to UInt64', (7, 13), (13, 6), (6, 1), (1, 12), (12, 8), (8, 5), (5, 4), (4, 11), (11, 10), (10, 2), (2, 9), (9, 3));
SELECT TreeHeight (from, to) FROM VALUES('from UInt64, to UInt64', (2, 14), (2, 8), (2, 7), (2, 9), (2, 11), (2, 10), (2, 3), (2, 15), (2, 6), (2, 13), (2, 5), (2, 1), (2, 4), (2, 12));
SELECT TreeHeight (from, to) FROM VALUES('from UInt64, to UInt64', (11, 2), (2, 1), (1, 8), (8, 4), (4, 3), (2, 7), (4, 6), (6, 9), (7, 10), (8, 5));
SELECT TreeHeight (from, to) FROM VALUES('from UInt64, to UInt64', (12, 14), (14, 10), (12, 1), (1, 13), (1, 7), (14, 3), (1, 6), (13, 8), (14, 11), (8, 5), (8, 4), (5, 15), (5, 9), (3, 2));
SELECT TreeHeight (from, to) FROM VALUES('from UInt64, to UInt64', (11, 2), (11, 6), (2, 3), (2, 13), (11, 8), (3, 14), (13, 9), (8, 1), (14, 12), (12, 15), (14, 5), (3, 4), (14, 7), (1, 10));