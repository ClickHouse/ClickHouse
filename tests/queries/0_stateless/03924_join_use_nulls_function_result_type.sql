DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3_fuzz;

CREATE TABLE t1 (id String, val String) ENGINE = Memory;
CREATE TABLE t2 (id String) ENGINE = Memory;
CREATE TABLE t3_fuzz (id Nullable(String), code Nullable(String)) ENGINE = Memory;

INSERT INTO t1 VALUES ('a', '1'), ('b', '2');
INSERT INTO t2 VALUES ('a');
INSERT INTO t3_fuzz VALUES ('b', 'x');

SET join_use_nulls = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT concat(t1.id, '_1') AS id, t1.val
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3_fuzz USING (id)
ORDER BY t1.val ASC;

SELECT concat(t2.id, '_suffix') AS result, toTypeName(concat(t2.id, '_suffix'))
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
ORDER BY t1.val ASC;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3_fuzz;
