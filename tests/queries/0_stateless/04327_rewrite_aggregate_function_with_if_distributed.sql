-- Tags: distributed

-- Reproducer for https://github.com/ClickHouse/ClickHouse/issues/82941
-- optimize_rewrite_aggregate_function_with_if + distributed + analyzer
-- The CAST inserted by the optimization must produce consistent column names
-- between the initiator and secondary (remote) servers.

SET enable_analyzer = 1;
SET optimize_rewrite_aggregate_function_with_if = 1;

SELECT sum(CASE WHEN dummy = 1 THEN 0 ELSE null END) FROM remote('127.0.0.{1,2}', system.one);
SELECT avg(CASE WHEN dummy = 1 THEN 0 ELSE null END) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum(if(dummy = 1, 0, null)) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum(if(dummy = 0, null, 1)) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum(if(dummy = 0, 42, null)) FROM remote('127.0.0.{1,2}', system.one);
