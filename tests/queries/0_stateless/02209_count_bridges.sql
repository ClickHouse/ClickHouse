SELECT GraphCountBridges (from, to) FROM (SELECT * FROM VALUES('from UInt64, to UInt64', (1, 1)) WHERE 0);
SELECT GraphCountBridges (from, to) FROM VALUES('from UInt64, to UInt64',(1, 4),(5, 1),(7, 6),(4, 10),(5, 11),(7, 8),(4, 13),(11, 6),(9, 5),(3, 7),(12, 3),(7, 2),(1, 4),(5, 2),(13, 4),(4, 12));
SELECT GraphCountBridges (from, to) FROM VALUES('from UInt64, to UInt64',(6, 4),(6, 10),(6, 2),(6, 14),(6, 3),(6, 9),(6, 1),(6, 16),(6, 7),(6, 13),(6, 12),(6, 15),(6, 5),(6, 8),(6, 11),(6, 17),(3, 14),(1, 4));
SELECT GraphCountBridges (from, to) FROM VALUES('from UInt64, to UInt64',(7, 17),(17, 15),(7, 14),(14, 3),(17, 2),(3, 11),(14, 5),(3, 12),(17, 10),(17, 16),(14, 4),(15, 6),(12, 13),(7, 8),(13, 1),(14, 9));
SELECT GraphCountBridges (from, to) FROM VALUES('from UInt64, to UInt64',(9, 16),(9, 7),(7, 5),(5, 11),(16, 3),(7, 2),(3, 6),(6, 10),(16, 13),(6, 17),(13, 4),(6, 12),(3, 8),(10, 1),(7, 15),(6, 14),(11, 1));
SELECT GraphCountBridges (from, to) FROM VALUES('from UInt64, to UInt64',(4, 8),(8, 7),(7, 11),(11, 10),(10, 1),(1, 9),(9, 3),(3, 6),(6, 2),(2, 5),(11, 6),(7, 8),(2, 6),(10, 7),(4, 7));
