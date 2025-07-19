-- Test short circuit evaluation for OR and AND operators
DROP TABLE IF EXISTS test_03562;
CREATE TABLE test_03562 (id UInt32) ENGINE = Memory;

insert into test_03562 select number from numbers(10000);

set enable_analyzer = 1;

-- Test OR short circuit with 1 (true) first operand
SELECT 1 where 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool;
SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool;

-- Test AND short circuit with 0 (false) first operand  
SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool;
SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool;


-- Test with string literals
SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool;
SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool;

SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool;
SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool;

-- Check the read_rows of the above queries to ensure that the short circuit is working
SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS test_03562;

