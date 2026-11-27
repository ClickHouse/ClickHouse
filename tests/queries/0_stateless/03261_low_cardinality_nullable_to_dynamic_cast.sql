SET allow_suspicious_low_cardinality_types = 1, allow_experimental_dynamic_type = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 LowCardinality(Nullable(Int))) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (NULL);
SELECT c0::Dynamic FROM t0;
SELECT c0 FROM t0;
DROP TABLE t0;
