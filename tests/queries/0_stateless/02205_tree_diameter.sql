SELECT treeDiameter(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (3, 4), (4, 5), (5, 1), (1, 6));
SELECT treeDiameter(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (1, 3), (1, 4), (1, 5));
SELECT treeDiameter(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (1, 3), (1, 5), (1, 4), (5, 6), (3, 7), (5, 8), (5, 9), (7, 10));
SELECT treeDiameter(from, to) FROM VALUES('from UInt64, to UInt64', (1, 2), (2, 3), (3, 4), (4, 5), (3, 6), (6, 7));
SELECT treeDiameter (from, to) FROM VALUES('from UInt64, to UInt64',(5, 4),(8, 2),(1, 8),(7, 2),(6, 5),(3, 8),(11, 6),(1, 13),(7, 9),(5, 3),(6, 10),(12, 4));
SELECT treeDiameter (from, to) FROM VALUES('from UInt64, to UInt64',(3, 11),(3, 15),(3, 13),(3, 17),(3, 4),(3, 12),(3, 6),(3, 8),(3, 9),(3, 7),(3, 14),(3, 1),(3, 10),(3, 18),(3, 2),(3, 16),(3, 5));
SELECT treeDiameter (from, to) FROM VALUES('from UInt64, to UInt64',(12, 5),(5, 13),(5, 7),(13, 4),(5, 2),(5, 10),(12, 3),(2, 9),(5, 1),(2, 8),(10, 11),(3, 6));
SELECT treeDiameter (from, to) FROM VALUES('from UInt64, to UInt64',(4, 2),(2, 8),(8, 7),(7, 10),(10, 6),(6, 5),(5, 1),(1, 9),(9, 3));
