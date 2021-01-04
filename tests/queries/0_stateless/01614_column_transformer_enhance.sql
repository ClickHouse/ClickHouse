DROP TABLE IF EXISTS default.t1;
DROP TABLE IF EXISTS default.t2;

CREATE TABLE default.t1(i Int64, j Int16, k Int64) Engine=TinyLog;
CREATE TABLE default.t2(i Int64, q Int64) Engine=TinyLog;
INSERT INTO default.t1 VALUES (1, 2, 3), (4, 5, 6);
INSERT INTO default.t2 VALUES (111, 222), (333, 444);

---------- EXCEPT -------------

SELECT * EXCEPT(t1.i) from default.t1;
SELECT * EXCEPT STRICT (table.i) from default.t1; -- { serverError 16 }
SELECT * EXCEPT(t1.i) from default.t1;
SELECT * EXCEPT STRICT (t1.i, t2.q) from default.t1, default.t2;
SELECT * EXCEPT(a.i, q)  from default.t1 a, default.t2;
SELECT * EXCEPT STRICT (default.a.i, q)  from default.t1 a, default.t2;
SELECT * EXCEPT STRICT (db.a.i, q)  from default.t1 a, default.t2; -- { serverError 16 }
SELECT * EXCEPT STRICT (a.i, db.t2.i)  from default.t1 a, default.t2; -- { serverError 16 }

---------- REPLACE -------------
select * REPLACE STRICT (t1.i+1 as t1.i) from default.t1;
select * REPLACE STRICT (t1.i+1 as default.t1.i) from default.t1;
select * REPLACE (t.i+1 as i) from default.t1; -- { serverError 47 }
select * REPLACE STRICT (i+1 as t.i) from default.t1; -- {serverError 16 }
select * REPLACE (t1.i+1 as i, default.t2.i-1 as default.t2.i) from default.t1, default.t2;
select * REPLACE (default.t1.i+1 as i, default2.t2.i-1 as default2.t2.i) from default.t1, default.t2;
select * REPLACE STRICT (default.t1.i+1 as i, default2.t2.i-1 as default2.t2.i) from default.t1, default.t2; -- { serverError 16 }
select * REPLACE (t1.i+1 as i, default2.t2.i-1 as default.t2.i) from default.t1, default.t2;  -- { serverError 47 }
select * REPLACE STRICT (t1.i+1 as t2.i, default.t2.i-1 as default2.t2.i) from default.t1, default.t2; -- { serverError 16 }
SELECT COLUMNS(t1.i, t1.j, t2.i) replace (t1.i+1 as t1.i, t2.i - 1 as t2.i ) from default.t1, default.t2;

DROP TABLE default.t1;
DROP TABLE default.t2;
