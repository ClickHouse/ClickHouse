DROP TABLE IF EXISTS t1__fuzz_4;
DROP TABLE IF EXISTS t0__fuzz_29;

SET allow_suspicious_low_cardinality_types = 1, join_algorithm = 'partial_merge', join_use_nulls = 1;
CREATE TABLE t1__fuzz_4 (`x` Nullable(UInt32), `y` Int64) ENGINE = MergeTree ORDER BY (x, y) SETTINGS allow_nullable_key = 1;
CREATE TABLE t0__fuzz_29 (`x` LowCardinality(UInt256), `y` Array(Array(Date))) ENGINE = MergeTree ORDER BY (x, y);
SELECT sum(0), NULL FROM t0__fuzz_29 FULL OUTER JOIN t1__fuzz_4 USING (x) PREWHERE NULL;

DROP TABLE t1__fuzz_4;
DROP TABLE t0__fuzz_29;
