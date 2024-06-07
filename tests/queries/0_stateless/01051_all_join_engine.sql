DROP TABLE IF EXISTS t1;

DROP TABLE IF EXISTS left_join;
DROP TABLE IF EXISTS inner_join;
DROP TABLE IF EXISTS right_join;
DROP TABLE IF EXISTS full_join;

CREATE TABLE t1 (x UInt32, str String) engine = Memory;

CREATE TABLE left_join (x UInt32, s String) engine = Join(ALL, LEFT, x);
CREATE TABLE inner_join (x UInt32, s String) engine = Join(ALL, INNER, x);
CREATE TABLE right_join (x UInt32, s String) engine = Join(ALL, RIGHT, x);
CREATE TABLE full_join (x UInt32, s String) engine = Join(ALL, FULL, x);

INSERT INTO t1 (x, str) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');

INSERT INTO left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO full_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SET join_use_nulls = 0;

SELECT 'left';
SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s;

SELECT 'inner';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

SELECT 'full';
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s;

SET join_use_nulls = 1;

SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT 'inner (join_use_nulls mix)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls mix)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

DROP TABLE left_join;
DROP TABLE inner_join;
DROP TABLE right_join;
DROP TABLE full_join;

CREATE TABLE left_join (x UInt32, s String) engine = Join(ALL, LEFT, x) SETTINGS join_use_nulls = 1;
CREATE TABLE inner_join (x UInt32, s String) engine = Join(ALL, INNER, x) SETTINGS join_use_nulls = 1;
CREATE TABLE right_join (x UInt32, s String) engine = Join(ALL, RIGHT, x) SETTINGS join_use_nulls = 1;
CREATE TABLE full_join (x UInt32, s String) engine = Join(ALL, FULL, x) SETTINGS join_use_nulls = 1;

INSERT INTO left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO full_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SELECT 'left (join_use_nulls)';
SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s;

SELECT 'inner (join_use_nulls)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

SELECT 'full (join_use_nulls)';
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s;

SET join_use_nulls = 0;

SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT 'inner (join_use_nulls mix2)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls mix2)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

DROP TABLE t1;

DROP TABLE left_join;
DROP TABLE inner_join;
DROP TABLE right_join;
DROP TABLE full_join;
