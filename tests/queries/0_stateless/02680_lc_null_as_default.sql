drop table if exists test_null_as_default__fuzz_46;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE test_null_as_default__fuzz_46 (a Nullable(DateTime64(3)), b LowCardinality(Float32) DEFAULT a + 1000) ENGINE = Memory;
INSERT INTO test_null_as_default__fuzz_46 SELECT 1, NULL UNION ALL SELECT 2, NULL;
drop table test_null_as_default__fuzz_46;

