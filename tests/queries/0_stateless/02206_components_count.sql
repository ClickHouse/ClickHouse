SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (3, 4), (4, 5), (5, 1), (1, 6));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (2, 3), (3, 4), (4, 5), (3, 6), (6, 7), (5, 6), (1, 4), (2, 7), (10, 11), (11, 12), (11, 16), (13, 14));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (4, 3), (5, 6), (7, 7));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (7, 13), (13, 6), (6, 1), (1, 12), (12, 8), (8, 5), (5, 4), (4, 11), (11, 10), (10, 2), (2, 9), (9, 3));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (2, 14), (2, 8), (2, 7), (2, 9), (2, 11), (2, 10), (2, 3), (2, 15), (2, 6), (2, 13), (2, 5), (2, 1), (2, 4), (2, 12));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (11, 2), (2, 1), (1, 8), (8, 4), (4, 3), (2, 7), (4, 6), (6, 9), (7, 10), (8, 5));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (6, 15), (3, 4), (11, 1), (5, 8), (14, 1), (2, 12), (11, 9), (10, 13), (12, 4), (14, 4), (6, 2), (10, 15), (13, 8), (4, 7));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (7, 12), (1, 5), (13, 2), (8, 8), (6, 8), (8, 10), (3, 6), (12, 9), (1, 1), (4, 3));
SELECT GraphComponentsCount (from, to) FROM VALUES('from UInt64, to UInt64', (9, 11), (9, 11), (9, 12), (14, 1), (12, 15), (14, 6), (10, 15), (4, 14), (16, 14), (10, 16), (1, 10), (17, 4), (7, 7), (17, 8), (13, 4), (6, 12));