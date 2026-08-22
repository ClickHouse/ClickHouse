DROP TABLE IF EXISTS my_events;
CREATE TABLE my_events (start UInt32, end UInt32) Engine = MergeTree ORDER BY tuple()
    AS Select * FROM VALUES ('start UInt32, end UInt32', (1, 3), (1, 6), (2, 5), (3, 7));
SELECT start, end, maxIntersections(start, end) OVER () FROM my_events;
SELECT start, end, maxIntersectionsPosition(start, end) OVER () FROM my_events;
