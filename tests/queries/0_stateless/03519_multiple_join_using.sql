SET enable_analyzer=1;

CREATE TABLE table0 (id Int64, val String) Engine=MergeTree ORDER BY id;
CREATE TABLE table1 (id2 Int64, val String) Engine=MergeTree ORDER BY id2;
CREATE TABLE table2 (id Int64, id2 Int64, val String) Engine=MergeTree ORDER BY (id, id2);

INSERT INTO table0 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO table1 VALUES (1, 'a'), (2,'b'), (3, 'c');
INSERT INTO table2 VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 2, 'b'), (1, 3, 'c'),  (3, 2, 'b');

-- { echoOn }
SELECT * FROM table0 JOIN table2 USING id JOIN table1 USING id2 ORDER BY ALL;

SELECT * FROM table0 AS t0 JOIN table2 USING val JOIN table1 USING val ORDER BY ALL;

WITH t0 AS (
    SELECT * FROM table0 WHERE val LIKE 'b%'
)
SELECT * FROM t0 JOIN table2 AS t2 USING id JOIN table1 AS t1 USING id2 ORDER BY ALL;
