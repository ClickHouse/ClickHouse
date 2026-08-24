SET enable_query_plan_cache = 1;
SET enable_parallel_replicas = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 VALUES (1), (2);

-- An unqualified `additional_table_filters` key applies only when the table is in
-- the session current database, even if the query itself uses a fully qualified name.
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'};

-- Check the opposite cache insertion order with a different query AST.
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'};
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'};

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
