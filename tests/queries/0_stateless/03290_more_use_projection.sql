CREATE TABLE test(
  date DateTime,
  user_id UInt64,
  user_name String,
  PROJECTION p1(SELECT * ORDER BY user_name, date)
)
ENGINE = MergeTree
ORDER BY date;

INSERT INTO test SELECT
  toDateTime('2024-12-01') + number as date,
  modulo(number, 1000) as user_id,
  concat('John#', user_id) as user_name
FROM (SELECT * FROM loop(numbers(100000)) as number LIMIT 100000);

SELECT * FROM test WHERE user_name = 'John#999' ORDER BY date LIMIT 10
SETTINGS optimize_use_projections=1, force_optimize_projection=1;
