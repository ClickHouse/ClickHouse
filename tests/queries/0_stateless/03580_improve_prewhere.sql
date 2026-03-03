-- Tags: no-fasttest
set optimize_move_to_prewhere = 1;
set move_all_conditions_to_prewhere = 1;
set enable_multiple_prewhere_read_steps = 1;
set move_primary_key_columns_to_end_of_prewhere = 1;
set allow_reorder_prewhere_conditions = 1;
set enable_analyzer = 1;
set enable_parallel_replicas = 0;
set allow_experimental_statistics = 1;
set allow_statistics_optimize = 1;

DROP TABLE IF EXISTS test_improve_prewhere;

CREATE TABLE test_improve_prewhere (
    primary_key String STATISTICS(CountMin),
    normal_column String STATISTICS(CountMin),
    value UInt32 STATISTICS(TDigest),
    date Date STATISTICS(CountMin),
) ENGINE = MergeTree()
ORDER BY primary_key;

INSERT INTO test_improve_prewhere
SELECT
    hex(number % 100) AS primary_key,
    arrayElement(['hello', 'world', 'test', 'example', 'sample'], number % 5 + 1) AS normal_column,
    number % 1000 + 1 AS value,
    toDate('2025-08-01') + number AS date
FROM numbers(100000);

-- { echoOn }
-- Condition: lower(primary_key) = '00' can't make use of primary key index. It shouldn't be moved to the end of prewhere conditions.
select trimLeft(explain) from (
EXPLAIN actions=1
SELECT * FROM test_improve_prewhere
WHERE date = '2025-08-05' and lower(primary_key) = '00' and normal_column != 'hello' and value < 100
) where explain ilike '%Prewhere filter column%';

-- Condition: primary_key = '00' can use primary key index. It should be moved to the end of prewhere conditions.
select trimLeft(explain) from (
EXPLAIN actions=1
SELECT * FROM test_improve_prewhere
WHERE date = '2025-08-05' and primary_key = '00' and normal_column != 'hello' and value < 100
) where explain ilike '%Prewhere filter column%';

-- Condition: lower(primary_key) IN ('00', '01') should be placed before Condition: normal_column != 'hello' and value < 100
-- because it has a lower estimated selectivity.
select trimLeft(replaceRegexpAll(explain, '__set_String_\\d+_\\d+', '__set_String')) from (
EXPLAIN actions=1
SELECT * FROM test_improve_prewhere
WHERE date = '2025-08-05' and lower(primary_key) IN ('00', '01') and normal_column != 'hello' and value < 100
) where explain ilike '%Prewhere filter column%';
-- { echoOff }

DROP TABLE test_improve_prewhere;
