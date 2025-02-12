DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x UInt32, s String) engine = Memory;
CREATE TABLE t2 (x UInt32, s String) engine = Memory;

INSERT INTO t1 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');
INSERT INTO t2 (x, s) VALUES (2, 'b1'), (4, 'b2'), (5, 'b4');

SET join_algorithm = 'prefer_partial_merge';
SET join_use_nulls = 0;
SET any_join_distinct_right_table_keys = 0;

SELECT 'any left';
SELECT t1.*, t2.* FROM t1 ANY LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any left (rev)';
SELECT t1.*, t2.* FROM t2 ANY LEFT JOIN t1 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any inner';
SELECT t1.*, t2.* FROM t1 ANY INNER JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any inner (rev)';
SELECT t1.*, t2.* FROM t2 ANY INNER JOIN t1 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any right';
SELECT t1.*, t2.* FROM t1 ANY RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any right (rev)';
SELECT t1.*, t2.* FROM t2 ANY RIGHT JOIN t1 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi left';
SELECT t1.*, t2.* FROM t1 SEMI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi right';
SELECT t1.*, t2.* FROM t1 SEMI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'anti left';
SELECT t1.*, t2.* FROM t1 ANTI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'anti right';
SELECT t1.*, t2.* FROM t1 ANTI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

DROP TABLE t1;
DROP TABLE t2;
