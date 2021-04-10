DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1(a String, b Date) ENGINE MergeTree order by a;
CREATE TABLE table2(c String, a String, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

set join_algorithm = 'partial_merge';

SELECT * FROM table1 AS t1 ALL LEFT JOIN (SELECT *, '0.10', c, d AS b FROM table2) AS t2 USING (a, b) ORDER BY d ASC FORMAT PrettyCompact settings max_rows_in_join = 1;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
