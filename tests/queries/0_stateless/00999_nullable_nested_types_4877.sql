DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a String, b Tuple(String, String)) ENGINE = Memory();
CREATE TABLE r (a String, c Tuple(String, String)) ENGINE = Memory();

INSERT INTO l (a, b) VALUES ('a', ('b', 'c')), ('d', ('e', 'f'));
INSERT INTO r (a, c) VALUES ('a', ('b', 'c')), ('x', ('y', 'z'));

SET join_use_nulls = 0;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

SET join_use_nulls = 1;
SELECT a from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

DROP TABLE l;
DROP TABLE r;

CREATE TABLE l (a String, b String) ENGINE = Memory();
CREATE TABLE r (a String, c Array(String)) ENGINE = Memory();

INSERT INTO l (a, b) VALUES ('a', 'b'), ('d', 'e');
INSERT INTO r (a, c) VALUES ('a', ['b', 'c']), ('x', ['y', 'z']);

SET join_use_nulls = 0;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

SET join_use_nulls = 1;
SELECT a from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

DROP TABLE l;
DROP TABLE r;
