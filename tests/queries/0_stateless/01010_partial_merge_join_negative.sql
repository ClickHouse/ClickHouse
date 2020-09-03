DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);
CREATE TABLE t1 (x UInt32, y UInt64) engine = MergeTree ORDER BY (x,y);

SET join_algorithm = 'partial_merge';

SELECT 'all';

SELECT * FROM t0 LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 INNER JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 RIGHT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 FULL JOIN t1 ON t1.x = t0.x;

SELECT * FROM t0 LEFT JOIN t1 USING x;
SELECT * FROM t0 INNER JOIN t1 USING x;
SELECT * FROM t0 RIGHT JOIN t1 USING x;
SELECT * FROM t0 FULL JOIN t1 USING x;

SELECT 'cross';

SELECT * FROM t0 CROSS JOIN t1; -- { serverError 48 }

SELECT 'any';

SELECT * FROM t0 ANY LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 ANY INNER JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 ANY RIGHT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 ON t1.x = t0.x; -- { serverError 48 }

SELECT * FROM t0 ANY LEFT JOIN t1 USING (x);
SELECT * FROM t0 ANY INNER JOIN t1 USING (x);
SELECT * FROM t0 ANY RIGHT JOIN t1 USING (x); -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 USING (x); -- { serverError 48 }

SELECT 'semi';

SELECT * FROM t0 SEMI LEFT JOIN t1 ON t1.x = t0.x;
SELECT * FROM t0 SEMI RIGHT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }

SELECT * FROM t0 SEMI LEFT JOIN t1 USING (x);
SELECT * FROM t0 SEMI RIGHT JOIN t1 USING (x); -- { serverError 48 }

SELECT 'anti';

SELECT * FROM t0 ANTI LEFT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT * FROM t0 ANTI RIGHT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }

SELECT * FROM t0 ANTI LEFT JOIN t1 USING (x); -- { serverError 48 }
SELECT * FROM t0 ANTI RIGHT JOIN t1 USING (x); -- { serverError 48 }

SELECT 'asof';

SELECT * FROM t0 ASOF LEFT JOIN t1 ON t1.x = t0.x AND t0.y > t1.y; -- { serverError 48 }
SELECT * FROM t0 ASOF LEFT JOIN t1 USING (x, y); -- { serverError 48 }

DROP TABLE t0;
DROP TABLE t1;
