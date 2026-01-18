-- Force new analyzer because the old one doesn't support multiple USING clauses in a query
SET allow_suspicious_low_cardinality_types = 1, enable_analyzer = 1;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t0 (x Int, y LowCardinality(Nullable(Int)) ALIAS x) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t1 (y LowCardinality(Int)) ENGINE = MergeTree ORDER BY y;
CREATE TABLE t2 (y Nullable(Int)) ENGINE = MergeTree ORDER BY y SETTINGS allow_nullable_key = 1;

SELECT t1.* FROM t0 FULL JOIN t1 USING (y) JOIN t2 USING (y) PREWHERE toLowCardinality(1);

DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;
