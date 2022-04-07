SELECT GraphAvgChildren(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (3, 4), (4, 5), (5, 1), (1, 6));
SELECT GraphAvgChildren(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (1, 3), (1, 4), (1, 5));
SELECT GraphAvgChildren(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (1, 3), (1, 5), (1, 4), (5, 6), (3, 7), (5, 8), (5, 9), (7, 10));
SELECT GraphAvgChildren(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (2, 3), (3, 4), (4, 5), (3, 6), (6, 7));
