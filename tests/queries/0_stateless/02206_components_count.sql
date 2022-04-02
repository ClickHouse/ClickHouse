SELECT graphComponentsCount(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (3, 4), (4, 5), (5, 1), (1, 6));
SELECT graphComponentsCount(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (2, 3), (3, 4), (4, 5), (3, 6), (6, 7), (5, 6), (1, 4), (2, 7), (10, 11), (11, 12), (11, 16), (13, 14));
SELECT graphComponentsCount(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (4, 3), (5, 6), (7, 7));
