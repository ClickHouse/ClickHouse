DROP TABLE IF EXISTS ints;
CREATE TABLE ints (i64 Int64, i32 Int32) ENGINE = Memory;

SET join_algorithm = 'partial_merge';

INSERT INTO ints SELECT 1 AS i64, number AS i32 FROM numbers(2);

SELECT * FROM ints l LEFT JOIN ints r USING i64 ORDER BY l.i32, r.i32;
SELECT '-';
SELECT * FROM ints l INNER JOIN ints r USING i64 ORDER BY l.i32, r.i32;

DROP TABLE ints;
