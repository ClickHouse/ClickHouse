-- https://github.com/ClickHouse/ClickHouse/issues/75005
DROP TABLE IF EXISTS t1_04064;
DROP TABLE IF EXISTS t2_04064;
DROP TABLE IF EXISTS t3_04064;

CREATE TABLE t1_04064 ( key UInt32, a UInt32, attr String ) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_04064 ( key UInt32, a UInt32, attr String ) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t3_04064 ( key UInt32, a UInt32, attr String ) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1_04064 (key, a, attr) VALUES (1, 10, 'alpha'), (2, 15, 'beta'), (3, 20, 'gamma');
INSERT INTO t2_04064 (key, a, attr) VALUES (1, 5, 'ALPHA'), (2, 10, 'beta'), (4, 25, 'delta');
INSERT INTO t3_04064 (key, a, attr) VALUES (1, 10, 'alpha'), (2, 100, 'beta'), (3, 200, 'gamma');

SELECT * APPLY ( (x) -> (x + 1) ) FROM (
    SELECT COLUMNS('a$')
    FROM t1_04064
    RIGHT JOIN t2_04064
    ON t1_04064.key = t2_04064.key
    RIGHT JOIN t3_04064
    ON t1_04064.key = t3_04064.key
) ORDER BY ALL
SETTINGS join_use_nulls = 1;

DROP TABLE t1_04064;
DROP TABLE t2_04064;
DROP TABLE t3_04064;
