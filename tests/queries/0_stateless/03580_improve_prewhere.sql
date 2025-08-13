-- Default settings
set optimize_move_to_prewhere = 1;
set move_all_conditions_to_prewhere = 1;
set enable_multiple_prewhere_read_steps = 1;
set move_primary_key_columns_to_end_of_prewhere = 1;
set allow_reorder_prewhere_conditions = 1;

set allow_statistics_optimize = 1;

DROP TABLE IF EXISTS test_improve_prewhere;

CREATE TABLE test_improve_prewhere (
    primary_key String,
    normal_column String,
    value UInt32,
    date Date
) ENGINE = MergeTree()
ORDER BY primary_key
PARTITION BY toYYYYMM(date);

INSERT INTO test_improve_prewhere
SELECT
    hex(rand() % 10) AS primary_key,
    arrayElement(['hello', 'world', 'test', 'example', 'sample'], rand() % 5 + 1) AS normal_column,
    rand() % 1000 + 1 AS value,
    toDate('2025-08-01') + (rand() % 10) AS date
FROM numbers(100000);

-- { echoOn }
set allow_statistics_optimize = 1;
-- Condition: lower(primary_key) = '00' can't make use of primary key index. It shouldn't be moved to the end of prewhere conditions.
EXPLAIN actions=1
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and lower(primary_key) = '00' and normal_column != 'hello' and value < 100;

-- Condition: primary_key = '00' can use primary key index. It should be moved to the end of prewhere conditions.
EXPLAIN actions=1 
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and primary_key = '00' and normal_column != 'hello' and value < 100;

-- Condition: lower(primary_key) IN ('00', '01') should be placed before Condition: normal_column != 'hello' and value < 100
-- because it has a lower estimated selectivity.
EXPLAIN actions=1 
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and lower(primary_key) in '00' and normal_column != 'hello' and value < 100;


set allow_statistics_optimize = 0;
EXPLAIN actions=1
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and lower(primary_key) = '00' and normal_column != 'hello' and value < 100;

EXPLAIN actions=1 
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and primary_key = '00' and normal_column != 'hello' and value < 100;

EXPLAIN actions=1 
SELECT * FROM test_improve_prewhere 
WHERE date = '2025-08-05' and lower(primary_key) in '00' and normal_column != 'hello' and value < 100;
-- { echoOff }

DROP TABLE test_improve_prewhere;
