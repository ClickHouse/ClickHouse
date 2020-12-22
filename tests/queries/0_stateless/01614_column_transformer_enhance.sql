DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(i Int64, j Int16, k Int64) Engine=TinyLog;
CREATE TABLE t2(i Int64, q Int64) Engine=TinyLog;
INSERT INTO t1 VALUES (1, 2, 3), (4, 5, 6);
INSERT INTO t2 VALUES (111, 222), (333, 444);


SELECT * EXCEPT(t1.i) from t1;
SELECT * EXCEPT STRICT (table.i) from t1; -- { serverError 16 }
SELECT * EXCEPT(t1.i) from t1;
SELECT * EXCEPT STRICT (t1.i, t2.q) from t1, t2;
SELECT * EXCEPT(a.i, q)  from t1 a, t2;

DROP TABLE t1;
DROP TABLE t2;
