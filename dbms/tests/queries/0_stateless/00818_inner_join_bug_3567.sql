DROP TABLE IF EXISTS using1;
DROP TABLE IF EXISTS using2;

CREATE TABLE using1(a String, b DateTime) ENGINE=MergeTree order by a;
CREATE TABLE using2(c String, a String, d DateTime) ENGINE=MergeTree order by c;

INSERT INTO using1 VALUES ('a', '2018-01-01 00:00:00') ('b', '2018-01-01 00:00:00') ('c', '2018-01-01 00:00:00');
INSERT INTO using2 VALUES ('d', 'd', '2018-01-01 00:00:00') ('b', 'b', '2018-01-01 00:00:00') ('c', 'c', '2018-01-01 00:00:00');

SELECT * FROM using1 t1 ALL LEFT JOIN (SELECT *, c as a, d as b FROM using2) t2 USING (a, b) ORDER BY d;
SELECT * FROM using1 t1 ALL INNER JOIN (SELECT *, c as a, d as b FROM using2) t2 USING (a, b) ORDER BY d;

DROP TABLE using1;
DROP TABLE using2;
