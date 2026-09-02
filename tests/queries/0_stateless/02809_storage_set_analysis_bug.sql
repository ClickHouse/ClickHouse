DROP TABLE IF EXISTS test_set;
DROP TABLE IF EXISTS null_in__fuzz_6;

set allow_suspicious_low_cardinality_types = 1;

CREATE TABLE null_in__fuzz_6 (`dt` LowCardinality(UInt16), `idx` Int32, `i` Nullable(Int256), `s` Int32) ENGINE = MergeTree PARTITION BY dt ORDER BY idx;
insert into null_in__fuzz_6 select * from generateRandom() where i is not null limit 1;

SET transform_null_in = 0;

CREATE TABLE test_set (i Nullable(int)) ENGINE = Set();
INSERT INTO test_set VALUES (1), (NULL);

-- The random `Int256` does not fit the set's `Nullable(Int32)` key, so it is not a member -
-- the conversion no longer raises for it (#113854).
SELECT count() FROM null_in__fuzz_6 PREWHERE 71 WHERE i IN (test_set);

DROP TABLE test_set;
DROP TABLE null_in__fuzz_6;
