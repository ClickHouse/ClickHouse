DROP TABLE IF EXISTS default_join1;
DROP TABLE IF EXISTS default_join2;

CREATE TABLE default_join1(a Int64, b Int64) ENGINE=Memory;
CREATE TABLE default_join2(a Int64, b Int64) ENGINE=Memory;

INSERT INTO default_join1 VALUES(1, 1), (2, 2), (3, 3);
INSERT INTO default_join2 VALUES(3, 3), (4, 4);

SELECT a, b FROM default_join1 JOIN (SELECT a, b FROM default_join2) USING a ORDER BY b SETTINGS join_default_strictness='ANY';
