DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t0 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);
CREATE TABLE t1 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);
CREATE TABLE t2 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);

SET join_algorithm = 'prefer_partial_merge';
SET partial_merge_join_rows_in_right_blocks = 1;
SET any_join_distinct_right_table_keys = 1;

INSERT INTO t1 (x, y) VALUES (0, 0);
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
