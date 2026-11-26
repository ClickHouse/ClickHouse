-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106083
-- MergeTree ORDER BY ... LIMIT 1 equality previously lost the only matching row
-- after predicate materialization. Issue is closed (no longer reproduces on master);
-- this test guards against re-introduction.

DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS temp_1;

CREATE TABLE t3 (c0 Int32) ENGINE = MergeTree() ORDER BY (sqrt(c0)) PARTITION BY gcd(c0, c0) SETTINGS allow_suspicious_indices = 1;
INSERT INTO t3 VALUES (-444709010);

CREATE TABLE temp_1 (subq0_subq0_c0 Int32) ENGINE = Memory;
INSERT INTO temp_1
SELECT subq0.subq0_c0
FROM (SELECT t3.c0 AS subq0_c0 FROM t3) AS subq0
WHERE (((NOT ((CAST(subq0.subq0_c0 AS Int32)) IN (-9223372036854775808))))
   AND ((((abs((abs(subq0.subq0_c0))))) IS NOT NULL)));

SELECT temp_1.subq0_subq0_c0
FROM temp_1
WHERE (temp_1.subq0_subq0_c0 = (SELECT t3.c0 FROM t3 ORDER BY t3.c0 LIMIT 1));

DROP TABLE temp_1;
DROP TABLE t3;
