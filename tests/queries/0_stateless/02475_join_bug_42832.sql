DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE tab1 (a1 Int32, b1 Int32, val UInt64) ENGINE = MergeTree ORDER BY a1;
CREATE TABLE tab2 (a2 LowCardinality(Int32), b2 Int32) ENGINE = MergeTree ORDER BY a2;

INSERT INTO tab1 SELECT number, number, 1 from numbers(4);
INSERT INTO tab2 SELECT number + 2, number + 2 from numbers(4);

SELECT sum(val), count(val) FROM tab1 FULL OUTER JOIN tab2 ON b1 - 2 = a2 OR a1 = b2 SETTINGS join_use_nulls = 0;
SELECT sum(val), count(val) FROM tab1 FULL OUTER JOIN tab2 ON b1 - 2 = a2 OR a1 = b2 SETTINGS join_use_nulls = 1;

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;
