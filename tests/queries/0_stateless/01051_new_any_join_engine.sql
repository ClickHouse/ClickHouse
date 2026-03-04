DROP TABLE IF EXISTS t1;

DROP TABLE IF EXISTS any_left_join;
DROP TABLE IF EXISTS any_inner_join;
DROP TABLE IF EXISTS any_right_join;
DROP TABLE IF EXISTS any_full_join;

DROP TABLE IF EXISTS semi_left_join;
DROP TABLE IF EXISTS semi_right_join;
DROP TABLE IF EXISTS anti_left_join;
DROP TABLE IF EXISTS anti_right_join;

CREATE TABLE t1 (x UInt32, str String) engine = MergeTree ORDER BY tuple();

CREATE TABLE any_left_join (x UInt32, s String) engine = Join(ANY, LEFT, x);
CREATE TABLE any_inner_join (x UInt32, s String) engine = Join(ANY, INNER, x);
CREATE TABLE any_right_join (x UInt32, s String) engine = Join(ANY, RIGHT, x);

CREATE TABLE semi_left_join (x UInt32, s String) engine = Join(SEMI, LEFT, x);
CREATE TABLE semi_right_join (x UInt32, s String) engine = Join(SEMI, RIGHT, x);

CREATE TABLE anti_left_join (x UInt32, s String) engine = Join(ANTI, LEFT, x);
CREATE TABLE anti_right_join (x UInt32, s String) engine = Join(ANTI, RIGHT, x);

INSERT INTO t1 (x, str) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');

INSERT INTO any_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO any_inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO any_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

INSERT INTO semi_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO semi_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO anti_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO anti_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SET join_use_nulls = 0;
SET any_join_distinct_right_table_keys = 0;
SET parallel_replicas_local_plan=1;

SELECT 'any left';
SELECT * FROM t1 ANY LEFT JOIN any_left_join j USING(x) ORDER BY x, str, s;

SELECT 'any inner';
SELECT * FROM t1 ANY INNER JOIN any_inner_join j USING(x) ORDER BY x, str, s;

SELECT 'any right';
SELECT * FROM t1 ANY RIGHT JOIN any_right_join j USING(x) ORDER BY x, str, s;

SELECT 'semi left';
SELECT * FROM t1 SEMI LEFT JOIN semi_left_join j USING(x) ORDER BY x, str, s;

SELECT 'semi right';
SELECT * FROM t1 SEMI RIGHT JOIN semi_right_join j USING(x) ORDER BY x, str, s;

SELECT 'anti left';
SELECT * FROM t1 ANTI LEFT JOIN anti_left_join j USING(x) ORDER BY x, str, s;

SELECT 'anti right';
SELECT * FROM t1 ANTI RIGHT JOIN anti_right_join j USING(x) ORDER BY x, str, s;

-- run queries once more time (issue #16991)

SELECT 'any left';
SELECT * FROM t1 ANY LEFT JOIN any_left_join j USING(x) ORDER BY x, str, s;

SELECT 'any inner';
SELECT * FROM t1 ANY INNER JOIN any_inner_join j USING(x) ORDER BY x, str, s;

SELECT 'any right';
SELECT * FROM t1 ANY RIGHT JOIN any_right_join j USING(x) ORDER BY x, str, s;

SELECT 'semi left';
SELECT * FROM t1 SEMI LEFT JOIN semi_left_join j USING(x) ORDER BY x, str, s;

SELECT 'semi right';
SELECT * FROM t1 SEMI RIGHT JOIN semi_right_join j USING(x) ORDER BY x, str, s;

SELECT 'anti left';
SELECT * FROM t1 ANTI LEFT JOIN anti_left_join j USING(x) ORDER BY x, str, s;

SELECT 'anti right';
SELECT * FROM t1 ANTI RIGHT JOIN anti_right_join j USING(x) ORDER BY x, str, s;

DROP TABLE t1;

DROP TABLE any_left_join;
DROP TABLE any_inner_join;
DROP TABLE any_right_join;

DROP TABLE semi_left_join;
DROP TABLE semi_right_join;
DROP TABLE anti_left_join;
DROP TABLE anti_right_join;
