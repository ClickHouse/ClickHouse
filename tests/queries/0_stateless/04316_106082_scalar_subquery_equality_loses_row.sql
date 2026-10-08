-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106082
-- Scalar-subquery equality previously lost the only matching row after predicate
-- materialization. Issue is closed (no longer reproduces on master); this test
-- guards against re-introduction.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS temp_0;

CREATE TABLE t1 (c1 String, c2 String, c4 String, c5 String, c6 Int32) ENGINE = Memory();
CREATE TABLE t2 (c0 Int32) ENGINE = MergeTree() ORDER BY (sqrt(c0)) PARTITION BY c0 SETTINGS allow_suspicious_indices = 1;

INSERT INTO t1 VALUES ('', '', '', '', 0);
INSERT INTO t2 VALUES (-1497821983), (2082608730);

CREATE TABLE temp_0 (subq1_subq1_c0 Int32, subq0_subq0_c0 Int32) ENGINE = Memory;
INSERT INTO temp_0
SELECT subq1.subq1_c0, subq0.subq0_c0
FROM (SELECT t1.c6 AS subq0_c0 FROM t1 WHERE (upper(t1.c5)) NOT IN ('v싓f2')) AS subq0,
     (SELECT t2.c0 AS subq1_c0 FROM t2) AS subq1
WHERE NOT ((-(subq1.subq1_c0) - 1747450852) IN (710566731));

SELECT temp_0.subq1_subq1_c0, temp_0.subq0_subq0_c0
FROM temp_0
WHERE (temp_0.subq1_subq1_c0 = (
    SELECT CAST(t2.c0 AS Int32)
    FROM t1, t2
    WHERE ((ifNull(reverse(t1.c4), '539463033') = CAST(t1.c1 AS String))
       AND (lower(CAST(t1.c2 AS String)) IS NOT NULL)
       AND (t1.c1 = (
            SELECT t1.c1
            FROM t1, t2
            WHERE (ifNull(CAST(coalesce(t1.c5, '1635563191', '') AS String), '0.0057') = CAST(t1.c4 AS String))
            ORDER BY t1.c1 LIMIT 1)))
    ORDER BY CAST(t2.c0 AS Int32) LIMIT 1))
ORDER BY temp_0.subq1_subq1_c0;

DROP TABLE temp_0;
DROP TABLE t2;
DROP TABLE t1;
