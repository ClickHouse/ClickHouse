-- Negative zero compares equal to positive zero, so a LowCardinality dictionary maps both to the same
-- entry. A column sorted by value then has contiguous dictionary indexes, as LIMIT BY and DISTINCT expect.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS lcv;
CREATE TABLE lcv (x LowCardinality(Float64)) ENGINE = Memory;

-- Mix the single-value insert path (VALUES) with the bulk insert path (SELECT).
INSERT INTO lcv SELECT 0.0 FROM numbers(100);
INSERT INTO lcv VALUES (-0.0);
INSERT INTO lcv SELECT 0.0 FROM numbers(100);

-- All rows form one group, so LIMIT 1 BY x returns a single row.
SELECT x FROM lcv ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1;

SELECT count(), countDistinct(x) FROM lcv;

DROP TABLE lcv;

-- Float32, exercising the direct insert and DISTINCT paths.
DROP TABLE IF EXISTS lcv32;
CREATE TABLE lcv32 (x LowCardinality(Float32)) ENGINE = Memory;
INSERT INTO lcv32 VALUES (0.0)(-0.0)(1.0)(-0.0)(0.0);
SELECT DISTINCT x FROM lcv32 ORDER BY x;
DROP TABLE lcv32;
