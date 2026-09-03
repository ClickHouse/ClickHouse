SET allow_suspicious_low_cardinality_types = 1, allow_experimental_dynamic_type = 1, allow_dynamic_type_in_join_keys=1;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
CREATE TABLE t0 (c0 Dynamic) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c0 LowCardinality(Nullable(Int))) ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO TABLE t0 (c0) VALUES (1::LowCardinality(Int));
INSERT INTO TABLE t1 (c0) VALUES (1);

SELECT 1 FROM t0 JOIN t1 ON t0.c0 = t1.c0;

DROP TABLE t0;
DROP TABLE t1;

