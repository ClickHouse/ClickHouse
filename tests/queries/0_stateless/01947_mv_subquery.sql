SET log_queries=1;
SET log_profile_events=true;

CREATE TABLE src Engine=MergeTree ORDER BY id AS SELECT number as id, toInt32(1) as value FROM numbers(1);
CREATE TABLE dst (id UInt64, delta Int64) Engine=MergeTree ORDER BY id;

-- First we try with default values (https://github.com/ClickHouse/ClickHouse/issues/9587)
SET use_index_for_in_with_subqueries = 1;

CREATE MATERIALIZED VIEW src2dst_true TO dst AS
SELECT
       id,
       src.value - deltas_sum as delta
FROM src
LEFT JOIN
(
    SELECT id, sum(delta) as deltas_sum FROM dst
    WHERE id IN (SELECT id FROM src WHERE not sleepEachRow(0.001))
    GROUP BY id
) _a
USING (id);

-- Inserting 2 numbers should require 2 calls to sleep
INSERT into src SELECT number + 100 as id, 1 FROM numbers(2);

-- Describe should not need to call sleep
DESCRIBE ( SELECT '1947 #3 QUERY - TRUE',
                  id,
                  src.value - deltas_sum as delta
            FROM src
            LEFT JOIN
                (
                    SELECT id, sum(delta) as deltas_sum FROM dst
                    WHERE id IN (SELECT id FROM src WHERE not sleepEachRow(0.001))
                    GROUP BY id
                ) _a
                USING (id)
    ) FORMAT Null;


SYSTEM FLUSH LOGS;

SELECT '1947 #1 CHECK - TRUE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%CREATE MATERIALIZED VIEW src2dst_true%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT '1947 #2 CHECK - TRUE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%INSERT into src SELECT number + 100 as id, 1 FROM numbers(2)%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT '1947 #3 CHECK - TRUE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%DESCRIBE ( SELECT ''1947 #3 QUERY - TRUE'',%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

DROP TABLE src2dst_true;


-- Retry the same but using use_index_for_in_with_subqueries = 0

SET use_index_for_in_with_subqueries = 0;

CREATE MATERIALIZED VIEW src2dst_false TO dst AS
SELECT
    id,
    src.value - deltas_sum as delta
FROM src
LEFT JOIN
(
    SELECT id, sum(delta) as deltas_sum FROM dst
    WHERE id IN (SELECT id FROM src WHERE not sleepEachRow(0.001))
    GROUP BY id
) _a
USING (id);

-- Inserting 2 numbers should require 2 calls to sleep
INSERT into src SELECT number + 200 as id, 1 FROM numbers(2);

-- Describe should not need to call sleep
DESCRIBE ( SELECT '1947 #3 QUERY - FALSE',
                  id,
                  src.value - deltas_sum as delta
            FROM src
            LEFT JOIN
            (
                SELECT id, sum(delta) as deltas_sum FROM dst
                WHERE id IN (SELECT id FROM src WHERE not sleepEachRow(0.001))
                GROUP BY id
            ) _a
            USING (id)
    ) FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT '1947 #1 CHECK - FALSE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%CREATE MATERIALIZED VIEW src2dst_false%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT '1947 #2 CHECK - FALSE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%INSERT into src SELECT number + 200 as id, 1 FROM numbers(2)%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT '1947 #3 CHECK - FALSE' as test,
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds
FROM system.query_log
WHERE query like '%DESCRIBE ( SELECT ''1947 #3 QUERY - FALSE'',%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

DROP TABLE src2dst_false;

DROP TABLE src;
DROP TABLE dst;
