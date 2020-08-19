DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;

CREATE TABLE a (x UInt64) ENGINE = Memory;
CREATE TABLE b (x UInt64) ENGINE = Memory;
CREATE TABLE c (x UInt64) ENGINE = Memory;

SET enable_optimize_predicate_expression = 0;

SELECT a.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON a.x = c.x;

SELECT a.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

SELECT b.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

SELECT c.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
