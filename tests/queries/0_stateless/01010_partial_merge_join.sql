DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t0 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);
CREATE TABLE t1 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);
CREATE TABLE t2 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);

INSERT INTO t1 (x, y) VALUES (0, 0);

SET join_algorithm = 'prefer_partial_merge';
SET any_join_distinct_right_table_keys = 1;

SELECT 't join none using';
SELECT * FROM t1 ANY LEFT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 LEFT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 ANY INNER JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 INNER JOIN t0 USING (x) ORDER BY x;
SELECT 't join none on';
SELECT * FROM t1 ANY LEFT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 LEFT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 ANY INNER JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 INNER JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT 'none join t using';
SELECT * FROM t0 ANY LEFT JOIN t1 USING (x);
SELECT * FROM t0 LEFT JOIN t1 USING (x);
SELECT * FROM t0 ANY INNER JOIN t1 USING (x);
SELECT * FROM t0 INNER JOIN t1 USING (x);
SELECT 'none join t on';
SELECT * FROM t0 ANY LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 ANY INNER JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 INNER JOIN t1 ON t1.x = t0.x;
SELECT '/none';

SET join_use_nulls = 1;

SELECT 't join none using';
SELECT * FROM t1 ANY LEFT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 LEFT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 ANY INNER JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 INNER JOIN t0 USING (x) ORDER BY x;
SELECT 't join none on';
SELECT * FROM t1 ANY LEFT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 LEFT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 ANY INNER JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 INNER JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT 'none join t using';
SELECT * FROM t0 ANY LEFT JOIN t1 USING (x);
SELECT * FROM t0 LEFT JOIN t1 USING (x);
SELECT * FROM t0 ANY INNER JOIN t1 USING (x);
SELECT * FROM t0 INNER JOIN t1 USING (x);
SELECT 'none join t on';
SELECT * FROM t0 ANY LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 ANY INNER JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 INNER JOIN t1 ON t1.x = t0.x;
SELECT '/none';

INSERT INTO t1 (x, y) VALUES (1, 10) (2, 20);
INSERT INTO t1 (x, y) VALUES (4, 40) (3, 30);

INSERT INTO t2 (x, y) VALUES (4, 41) (2, 21) (2, 22);
INSERT INTO t2 (x, y) VALUES (0, 0) (5, 50) (4, 42);

SET join_use_nulls = 0;

SELECT 'any left';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x,y) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x,y) ORDER BY x;

SELECT 'all left';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND toUInt32(intDiv(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND toUInt64(t1.x) = intDiv(t2.y,10) ORDER BY x, t2.y;

SELECT 'any inner';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x,y) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x,y) ORDER BY x;

SELECT 'all inner';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND toUInt32(intDiv(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND toUInt64(t1.x) = intDiv(t2.y,10) ORDER BY x, t2.y;

SET join_use_nulls = 1;

SELECT 'any left';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x,y) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY LEFT JOIN t2 USING (x,y) ORDER BY x;

SELECT 'all left';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND toUInt32(intDiv(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND toUInt64(t1.x) = intDiv(t2.y,10) ORDER BY x, t2.y;

SELECT 'any inner';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x,y) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x) ORDER BY x;
SELECT '-';
SELECT t1.*, t2.x FROM t1 ANY INNER JOIN t2 USING (x,y) ORDER BY x;

SELECT 'all inner';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND toUInt32(intDiv(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.x = t2.x AND toUInt64(t1.x) = intDiv(t2.y,10) ORDER BY x, t2.y;

DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;
