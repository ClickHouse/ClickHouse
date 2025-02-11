-- Tags: no-fasttest
-- We test only that parsing of the endpoint works in this test.
CREATE OR REPLACE TABLE tablefunc01 (x int) AS postgresql('localhost:9005/postgresql', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE OR REPLACE TABLE tablefunc02 (x int) AS mysql('127.0.0.1:9004/mysql', 'mysql_db', 'mysql_table', 'mysql_user','123123');
