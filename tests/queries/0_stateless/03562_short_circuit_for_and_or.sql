-- Test short circuit evaluation for OR and AND operators
DROP TABLE IF EXISTS test_03562;
CREATE TABLE test_03562 (id UInt32) ENGINE = Memory;

insert into test_03562 select number from numbers(10000);

set enable_analyzer = 1;
set enable_function_early_short_circuit = 1;

SELECT 'Test OR short circuit with 1 (true) first operand';
SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- 1

SELECT 'Test AND short circuit with 0 (false) first operand';
SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- no result
SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- 0


SELECT 'Test with string literals';
SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- 1
SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- true

SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- no result
SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- false

SELECT 'Test with nested OR and AND';
SELECT 1 AND (1 OR (SELECT count(*) FROM test_03562) > 1) AS bool; -- 1
SELECT 0 OR (0 AND (SELECT count(*) FROM test_03562) > 1) AS bool; -- 0
SELECT true AND (true OR (SELECT count(*) FROM test_03562) > 1) AS bool; -- true
SELECT false OR (false AND (SELECT count(*) FROM test_03562) > 1) AS bool; -- false


SELECT 'Check the read_rows of the above queries to ensure that the short circuit is working';
SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where 0 OR ((SELECT count(*) FROM test_03562) > 1 AND 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 AND (1 OR (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 0 OR (0 AND (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT true AND (true OR (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT false OR (false AND (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS test_03562;

