DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key String, attr String, a UInt64, b UInt64, c Nullable(UInt64)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key String, attr String, a UInt64, b UInt64, c Nullable(UInt64)) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 VALUES ('key1', 'a', 1, 1, 2), ('key1', 'b', 2, 3, 2), ('key1', 'c', 3, 2, 1), ('key1', 'd', 4, 7, 2), ('key1', 'e', 5, 5, 5), ('key2', 'a2', 1, 1, 1), ('key4', 'f', 2, 3, 4);
INSERT INTO t2 VALUES ('key1', 'A', 1, 2, 1), ('key1', 'B', 2, 1, 2), ('key1', 'C', 3, 4, 5), ('key1', 'D', 4, 1, 6), ('key3', 'a3', 1, 1, 1), ('key4', 'F', 1,1,1);

SET allow_experimental_join_condition = true;
SET enable_analyzer = true;

SELECT t1.* FROM t1 FULL OUTER JOIN t2 ON t1.key = t2.key AND (t1.a = 2 OR indexHint(t2.a = 2)) FORMAT Null
;
