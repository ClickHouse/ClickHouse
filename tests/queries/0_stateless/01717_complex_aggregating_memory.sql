DROP TABLE IF EXISTS abc;
DROP TABLE IF EXISTS abc_sum;
DROP TABLE IF EXISTS tab;


CREATE TABLE abc(`a` UInt32, `b` UInt32, `c` UInt32) ENGINE = Memory;

CREATE TABLE abc_sum
ENGINE = AggregatingMemory()
AS SELECT
    sum(x + c)
FROM abc
WHERE ((a + b) as x) > 0;

INSERT INTO abc_sum VALUES (1, 2, 3), (0, 1, 0);
INSERT INTO abc_sum VALUES (2, 3, 4);
SELECT * FROM abc_sum;


CREATE TABLE tab ENGINE = AggregatingMemory() AS SELECT sum(number), number FROM numbers(1) GROUP BY number HAVING number > 1;
INSERT INTO tab SELECT number FROM numbers(3);
SELECT * FROM tab;
INSERT INTO tab VALUES (6), (6);
SELECT * FROM tab;


CREATE TABLE tab_join ENGINE = AggregatingMemory() AS SELECT sum(x), x FROM numbers(1) ARRAY JOIN [number] AS x GROUP BY x;
INSERT INTO tab_join SELECT number FROM numbers(4);
SELECT * FROM tab_join ORDER BY x;
