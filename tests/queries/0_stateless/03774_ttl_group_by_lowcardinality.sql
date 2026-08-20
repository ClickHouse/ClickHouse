-- Test TTL GROUP BY with LowCardinality columns in SET expression
-- This verifies the fix for type mismatch crash when using LowCardinality types

DROP TABLE IF EXISTS ttl_group_by__fuzz_13;

CREATE TABLE ttl_group_by__fuzz_13
(
    d Date,
    i LowCardinality(Float32),
    v LowCardinality(Nullable(UInt64))
)
ENGINE = MergeTree
ORDER BY (toStartOfMonth(d), i % 10)
TTL d + toIntervalYear(10) GROUP BY toStartOfMonth(d), i % 10 SET d = any(toStartOfMonth(d)), i = any(i % 10), v = sum(v), d + toIntervalYear(40) GROUP BY toStartOfMonth(d) SET d = any(toStartOfMonth(d)), v = sum(v)
SETTINGS allow_suspicious_low_cardinality_types = 1;

INSERT INTO ttl_group_by__fuzz_13 SELECT toDate('2000-10-10'), number, number FROM numbers(100);
INSERT INTO ttl_group_by__fuzz_13 SELECT toDate('1970-10-10'), number, number FROM numbers(100);

OPTIMIZE TABLE ttl_group_by__fuzz_13 FINAL;

SELECT d, v FROM ttl_group_by__fuzz_13 ORDER BY d, v;

DROP TABLE ttl_group_by__fuzz_13;
