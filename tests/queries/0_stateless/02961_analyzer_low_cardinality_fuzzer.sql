set allow_suspicious_low_cardinality_types = true;

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

INSERT INTO test_tuple_filter__fuzz_2 SELECT number, toString(number), toDate('2024-01-01') + number FROM numbers(10);

SELECT 
  (tuple(log_date) = tuple('2021-01-01'), log_date)
FROM test_tuple_filter__fuzz_2
ORDER BY log_date;
