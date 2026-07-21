SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET enable_parallel_replicas = 0;

DROP DATABASE IF EXISTS query_plan_cache_04603_db1;
DROP DATABASE IF EXISTS query_plan_cache_04603_db2;
CREATE DATABASE query_plan_cache_04603_db1;
CREATE DATABASE query_plan_cache_04603_db2;
CREATE TABLE query_plan_cache_04603_db1.t (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO query_plan_cache_04603_db1.t VALUES (1), (2);

-- An unqualified `additional_table_filters` key applies only when the table is in
-- the session current database, even if the query itself uses a fully qualified name.
USE query_plan_cache_04603_db1;
SELECT a FROM query_plan_cache_04603_db1.t ORDER BY a
SETTINGS additional_table_filters = {'t': 'a = 1'};
USE query_plan_cache_04603_db2;
SELECT a FROM query_plan_cache_04603_db1.t ORDER BY a
SETTINGS additional_table_filters = {'t': 'a = 1'};

-- Check the opposite cache insertion order with a different query AST.
SELECT a FROM query_plan_cache_04603_db1.t WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t': 'a = 1'};
USE query_plan_cache_04603_db1;
SELECT a FROM query_plan_cache_04603_db1.t WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t': 'a = 1'};

USE default;
DROP DATABASE query_plan_cache_04603_db1;
DROP DATABASE query_plan_cache_04603_db2;
