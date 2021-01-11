DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1(a String, b Date) ENGINE MergeTree order by a;
CREATE TABLE table2(c String, a String, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

SELECT * FROM table1 t1 FORMAT PrettyCompact;
SELECT *, c as a, d as b FROM table2 FORMAT PrettyCompact;
SELECT * FROM table1 t1 ALL LEFT JOIN (SELECT *, c, d as b FROM table2) t2 USING (a, b) ORDER BY d FORMAT PrettyCompact;
SELECT * FROM table1 t1 ALL INNER JOIN (SELECT *, c, d as b FROM table2) t2 USING (a, b) ORDER BY d FORMAT PrettyCompact;

DROP TABLE table1;
DROP TABLE table2;
