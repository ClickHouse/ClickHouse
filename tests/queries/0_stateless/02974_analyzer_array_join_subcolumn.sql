DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t2 (id Int32, pe Map(String, Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, {'a': (1, 2), 'b': (2, 3)}),;

CREATE TABLE t3 (id Int32, c Tuple(v String, pe Map(String, Tuple(a UInt64, b UInt64)))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t3 VALUES (1, ('A', {'a':(1, 2),'b':(2, 3)}));

SELECT pe, pe.values.a FROM (SELECT * FROM t2) ARRAY JOIN pe SETTINGS enable_analyzer = 1;
SELECT p, p.values.a FROM (SELECT * FROM t2) ARRAY JOIN pe AS p SETTINGS enable_analyzer = 1;

SELECT pe, pe.values.a FROM t2 ARRAY JOIN pe;
SELECT p, p.values.a FROM t2 ARRAY JOIN pe AS p;

SELECT c.pe, c.pe.values.a FROM (SELECT * FROM t3) ARRAY JOIN c.pe SETTINGS enable_analyzer = 1;
SELECT p, p.values.a FROM (SELECT * FROM t3) ARRAY JOIN c.pe as p SETTINGS enable_analyzer = 1;

SELECT c.pe, c.pe.values.a FROM t3 ARRAY JOIN c.pe SETTINGS enable_analyzer = 1;
SELECT p, p.values.a FROM t3 ARRAY JOIN c.pe as p;


DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
