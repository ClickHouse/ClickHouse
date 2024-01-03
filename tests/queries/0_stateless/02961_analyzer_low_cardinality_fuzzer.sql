set allow_suspicious_low_cardinality_types = true;
set allow_experimental_analyzer = 1;

CREATE TABLE test_tuple_filter__fuzz_2
(
  `id` Nullable(UInt32),
  `value` LowCardinality(String),
  `log_date` LowCardinality(Date)
)
ENGINE = MergeTree
PARTITION BY log_date
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_tuple_filter__fuzz_2 SELECT * FROM generateRandom() LIMIT 10;

SELECT 
  (tuple(log_date) = tuple('2021-01-01'), log_date)
FROM test_tuple_filter__fuzz_2;
