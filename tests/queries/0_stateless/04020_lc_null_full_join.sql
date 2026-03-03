-- https://github.com/ClickHouse/ClickHouse/issues/74288
SET allow_suspicious_low_cardinality_types = 1, join_use_nulls = 1, enable_analyzer = 1;
CREATE TABLE t0 (c0 LowCardinality(Int)) ENGINE = Memory;
SELECT 1 FROM (SELECT 1 AS c0) v0 FULL JOIN t0 ON (t0.c0 AS c0) IS NULL WHERE c0 = 1;
